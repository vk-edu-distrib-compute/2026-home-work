package company.vk.edu.distrib.compute.maryarta.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.maryarta.H2Dao;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicatedServiceImpl implements ReplicatedService {
    private final ReplicationService replicationService;
    private HttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();
    private final int port;
    private boolean started;
    private final H2Dao dao;
    private ExecutorService executor;
    private final int replicationFactor;


    public ReplicatedServiceImpl(int port, int replicationFactor, List<String> replicas, H2Dao dao) throws IOException {
        this.port = port;
        this.replicationFactor = replicationFactor;
        this.dao = dao;

        List<ReplicaNode> replicaNodes = new ArrayList<>();

        for (int i = 0; i < replicationFactor; i++) {
            H2Dao replicaDao = new H2Dao("node-" + port + "-replica-" + i);
            replicaNodes.add(new ReplicaNode(replicaDao));
        }

        this.replicationService = new ReplicationService(replicationFactor, replicaNodes);
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            executor = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(executor);
            createContext();
            server.start();
            started = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            return;
        }
        server.stop(0);
        if (executor != null) {
            executor.close();
        }
        started = false;
    }

    private void createContext() {
        server.createContext("/v0/status", handleStatusRequest());
        server.createContext("/v0/entity", handleEntityRequest());
    }


    private HttpHandler handleStatusRequest() {
        return exchange -> {
            String method = exchange.getRequestMethod();
            if ("GET".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        };
    }

    //GET /v0/entity?id=ID&ack=X
    private HttpHandler handleEntityRequest() {
        return exchange -> {
            try (exchange) {
                try {
                    String method = exchange.getRequestMethod();
                    String query = exchange.getRequestURI().getQuery();
                    String id = parseId(query);
//                    String target = replicationService;
                    int ack = 1;
                    if (query.contains("&")) {
                        String[] parts = query.split("&");
                        if (parts.length == 2) {
                            ack = parseACK(parts[1]);
                        }
                    }
                    if (ack > replicationFactor) {
                        throw new IllegalArgumentException();
                    }
                    switch (method) {
                        case "GET" -> {
                            byte[] value = replicationService.get(ack, id);
                            if(value == null){
                                exchange.sendResponseHeaders(503, 0);
                                return;
                            }
                            exchange.sendResponseHeaders(200, value.length);
                            exchange.getResponseBody().write(value);
                        }
                        case "PUT" -> {
                            byte[] newValue = exchange.getRequestBody().readAllBytes();
                            if(replicationService.put(ack,id,newValue)) {
                                exchange.sendResponseHeaders(201, 0);
                            } else exchange.sendResponseHeaders(503, 0);
                        }
                        case "DELETE" -> {
                            if(replicationService.delete(ack, id)){
                                exchange.sendResponseHeaders(202, 0);
                            } else exchange.sendResponseHeaders(503, 0);
                        }
                        default -> exchange.sendResponseHeaders(405, 0);
                    }
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, 0);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, 0);
                }
            }
        };
    }

    private void proxyRequest(HttpExchange exchange, String target) throws IOException {
        try {
            URI uri = URI.create(target + exchange.getRequestURI());
            HttpRequest.Builder request = HttpRequest.newBuilder(uri);
            switch (exchange.getRequestMethod()) {
                case "GET" -> request.GET();
                case "PUT" -> {
                    byte[] requestBody = exchange.getRequestBody().readAllBytes();
                    request.PUT(HttpRequest.BodyPublishers.ofByteArray(requestBody));
                }
                case "DELETE" -> request.DELETE();
                default -> {
                    exchange.sendResponseHeaders(405, 0);
                    return;
                }
            }
            HttpResponse<byte[]> response = client.send(
                    request.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            byte[] responseBody = response.body();
            if (responseBody == null) {
                responseBody = new byte[0];
            }
            exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
            exchange.getResponseBody().write(responseBody);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, 0);
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, 0);
        }
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    private static int parseACK(String query){
        if (query != null && query.startsWith("ack=")) {
            return Integer.parseInt(query.substring(4));
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicationService.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicationService.enableReplica(nodeId);
    }


}
