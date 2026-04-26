package company.vk.edu.distrib.compute.goshanchic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class KVServiceImpl implements ReplicatedService {
    private static final String PROXIED_HEADER = "X-Proxied";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String PARAM_ID = "id";
    private static final String PARAM_ACK = "ack";

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final int STATUS_SERVICE_UNAVAILABLE = 503;

    private final HttpServer server;
    private final InMemoryDao dao;
    private final List<String> clusterNodes;
    private final String selfAddress;
    private final HttpClient httpClient;

    private final int replicationFactor;
    private int defaultAck;

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao) throws IOException {
        this(port, allPorts, dao, 1, 1);
    }

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao,
                         int replicationFactor, int defaultAck) throws IOException {
        if (defaultAck > replicationFactor) {
            throw new IllegalArgumentException(
                    "ack (" + defaultAck + ") cannot exceed replicationFactor (" + replicationFactor + ")");
        }

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.selfAddress = "http://localhost:" + port;
        this.clusterNodes = allPorts.stream()
                .map(p -> "http://localhost:" + p)
                .collect(Collectors.toList());
        this.httpClient = HttpClient.newHttpClient();
        this.replicationFactor = Math.min(replicationFactor, clusterNodes.size());
        this.defaultAck = defaultAck;
        setupEndpoints();
    }

    @Override
    public int getReplicationFactor() {
        return replicationFactor;
    }

    @Override
    public int getAck() {
        return defaultAck;
    }

    @Override
    public void setAck(int ack) {
        if (ack > replicationFactor) {
            throw new IllegalArgumentException("ack cannot exceed replicationFactor");
        }
        this.defaultAck = ack;
    }

    private void setupEndpoints() {
        server.createContext("/v0/status", exchange -> {
            try {
                sendResponse(exchange, STATUS_OK, "OK".getBytes());
            } catch (IOException e) {
                exchange.close();
            }
        });

        server.createContext("/v0/entity", exchange -> {
            try {
                processEntityRequest(exchange);
            } catch (Exception e) {
                try {
                    sendResponse(exchange, STATUS_INTERNAL_ERROR, "Internal Server Error".getBytes());
                } catch (IOException ex) {
                    exchange.close();
                }
            }
        });
    }

    private void processEntityRequest(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String id = extractParam(query, PARAM_ID);
        int ack = extractAck(query);

        if (ack > replicationFactor) {
            sendResponse(exchange, STATUS_BAD_REQUEST,
                    ("Invalid ack: " + ack + " > " + replicationFactor).getBytes());
            return;
        }

        if (id == null || id.isEmpty()) {
            sendResponse(exchange, STATUS_BAD_REQUEST, "Bad Request".getBytes());
            return;
        }

        List<String> replicas = getReplicas(id);
        String method = exchange.getRequestMethod();

        switch (method) {
            case METHOD_GET:
                handleReplicatedGet(exchange, id, replicas, ack);
                break;
            case METHOD_PUT:
                handleReplicatedPut(exchange, id, replicas, ack);
                break;
            case METHOD_DELETE:
                handleReplicatedDelete(exchange, id, replicas, ack);
                break;
            default:
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed".getBytes());
                break;
        }
    }

    private List<String> getReplicas(String key) {
        return clusterNodes.stream()
                .sorted(Comparator.comparingLong((String node) -> {
                    int h1 = key.hashCode();
                    int h2 = node.hashCode();
                    return ((long) h1 << 32) | (h2 & 0xFFFFFFFFL);
                }).reversed())
                .limit(replicationFactor)
                .collect(Collectors.toList());
    }

    private void handleReplicatedGet(HttpExchange exchange, String id,
                                     List<String> replicas, int ack) throws IOException {
        List<byte[]> responses = new ArrayList<>();
        int successCount = 0;

        for (String replica : replicas) {
            try {
                ReplicaResponse response = getFromReplica(replica, id);
                if (response.found) {
                    responses.add(response.value.clone());
                }
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        if (responses.isEmpty()) {
            sendResponse(exchange, STATUS_NOT_FOUND, "Not Found".getBytes());
        } else {
            sendResponse(exchange, STATUS_OK, responses.getFirst());
        }
    }

    private void handleReplicatedPut(HttpExchange exchange, String id,
                                     List<String> replicas, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int successCount = 0;

        for (String replica : replicas) {
            try {
                putToReplica(replica, id, body);
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        sendResponse(exchange, STATUS_CREATED, "Created".getBytes());
    }

    private void handleReplicatedDelete(HttpExchange exchange, String id,
                                        List<String> replicas, int ack) throws IOException {
        int successCount = 0;

        for (String replica : replicas) {
            try {
                deleteFromReplica(replica, id);
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        sendResponse(exchange, STATUS_ACCEPTED, "Accepted".getBytes());
    }

    private record ReplicaResponse(byte[] value, boolean found) {
    }

    private ReplicaResponse getFromReplica(String replica, String id) throws IOException, InterruptedException {
        if (replica.equals(selfAddress)) {
            try {
                return new ReplicaResponse(dao.get(id), true);
            } catch (NoSuchElementException e) {
                return new ReplicaResponse(null, false);
            }
        }

        URI uri = URI.create(replica + "/v0/entity?id=" + id);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header(PROXIED_HEADER, "true")
                .GET()
                .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == STATUS_OK) {
            return new ReplicaResponse(response.body(), true);
        }
        return new ReplicaResponse(null, false);
    }

    private void putToReplica(String replica, String id, byte[] body) throws IOException, InterruptedException {
        if (replica.equals(selfAddress)) {
            dao.upsert(id, body);
            return;
        }

        URI uri = URI.create(replica + "/v0/entity?id=" + id);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header(PROXIED_HEADER, "true")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private void deleteFromReplica(String replica, String id) throws IOException, InterruptedException {
        if (replica.equals(selfAddress)) {
            dao.delete(id);
            return;
        }

        URI uri = URI.create(replica + "/v0/entity?id=" + id);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header(PROXIED_HEADER, "true")
                .DELETE()
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private int extractAck(String query) {
        String ackStr = extractParam(query, PARAM_ACK);
        if (ackStr != null) {
            try {
                return Integer.parseInt(ackStr);
            } catch (NumberFormatException e) {
                return defaultAck;
            }
        }
        return defaultAck;
    }

    private String extractParam(String query, String paramName) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2 && paramName.equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body != null ? body.length : -1);
        if (body != null && body.length > 0) {
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException ex) {
            // Closing DAO resource, exception can be safely ignored during shutdown
        }
    }
}








