package company.vk.edu.distrib.compute.goshanchic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternalServiceGrpc;
import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternal.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.GodClass")
public class KVServiceImpl implements ReplicatedService {
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

    private final HttpServer httpServer;
    private final GoshanchicGrpcServer grpcServer;
    private final InMemoryDao dao;
    private final List<String> clusterNodes;
    private final String selfAddress;
    private final Map<String, ManagedChannel> grpcChannels = new ConcurrentHashMap<>();

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

        this.dao = dao;
        this.selfAddress = "http://localhost:" + port;
        int grpcPort = port + 1000;
        this.clusterNodes = allPorts.stream()
                .map(p -> "http://localhost:" + p + "?grpcPort=" + (p + 1000))
                .collect(Collectors.toList());
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.grpcServer = new GoshanchicGrpcServer(grpcPort, dao);
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
        httpServer.createContext("/v0/status", exchange -> {
            try {
                sendResponse(exchange, STATUS_OK, "OK".getBytes());
            } catch (IOException e) {
                exchange.close();
            }
        });

        httpServer.createContext("/v0/entity", exchange -> {
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

        if (isInvalidAck(ack)) {
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

    private boolean isInvalidAck(int ack) {
        return ack > replicationFactor;
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

    private record ReplicaResponse(byte[] value, boolean found) {
    }

    private ReplicaResponse getFromReplica(String replica, String id) {
        if (replica.startsWith(selfAddress)) {
            try {
                return new ReplicaResponse(dao.get(id), true);
            } catch (NoSuchElementException | IOException e) {
                return new ReplicaResponse(null, false);
            }
        }
        return getFromReplicaGrpc(replica, id);
    }

    private void putToReplica(String replica, String id, byte[] body) {
        if (replica.startsWith(selfAddress)) {
            try {
                dao.upsert(id, body);
            } catch (IOException e) {
                // DAO exception
            }
            return;
        }
        putToReplicaGrpc(replica, id, body);
    }

    private void deleteFromReplica(String replica, String id) {
        if (replica.startsWith(selfAddress)) {
            try {
                dao.delete(id);
            } catch (IOException e) {
                // DAO exception
            }
            return;
        }
        deleteFromReplicaGrpc(replica, id);
    }

    private ReplicaResponse getFromReplicaGrpc(String replica, String id) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int port = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        GetRequest request = GetRequest.newBuilder().setId(id).build();
        GetResponse response = stub.get(request);

        if (response.getStatus() == STATUS_OK) {
            return new ReplicaResponse(response.getValue().toByteArray(), true);
        }
        return new ReplicaResponse(null, false);
    }

    private void putToReplicaGrpc(String replica, String id, byte[] body) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int port = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        PutRequest request = PutRequest.newBuilder()
                .setId(id)
                .setValue(com.google.protobuf.ByteString.copyFrom(body))
                .build();
        stub.put(request);
    }

    private void deleteFromReplicaGrpc(String replica, String id) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int port = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        DeleteRequest request = DeleteRequest.newBuilder().setId(id).build();
        stub.delete(request);
    }

    private String extractHost(String address) {
        return address.replace("http://", "").split(":")[0];
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
        httpServer.start();
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        httpServer.stop(0);
        grpcServer.stop();
        grpcChannels.values().forEach(channel -> {
            try {
                channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            dao.close();
        } catch (IOException ex) {
            // Closing DAO resource, exception can be safely ignored during shutdown
        }
    }
}







