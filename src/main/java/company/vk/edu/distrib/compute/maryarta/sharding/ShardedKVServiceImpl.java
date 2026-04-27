package company.vk.edu.distrib.compute.maryarta.sharding;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.maryarta.H2Dao;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ShardedKVServiceImpl implements KVService {
    private HttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final int port;
    private boolean started;
    private final ShardingStrategy shardingStrategy;
    private ExecutorService executor;

    public ShardedKVServiceImpl(int port, ShardingStrategy shardingStrategy) throws IOException {
        this.port = port;
        this.dao = new H2Dao("node-" + port);
        this.selfEndpoint = "http://localhost:" + port;
        this.shardingStrategy = shardingStrategy;
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

    private HttpHandler handleEntityRequest() {
        return exchange -> {
            try (exchange) {
                try {
                    String method = exchange.getRequestMethod();
                    String query = exchange.getRequestURI().getQuery();
                    String id = parseId(query);
                    String target = shardingStrategy.getEndpoint(parseId(query));
                    if (!target.equals(selfEndpoint)) {
                        proxyRequest(exchange, target);
                        return;
                    }
                    switch (method) {
                        case "GET" -> {
                            byte[] value = dao.get(id);
                            exchange.sendResponseHeaders(200, value.length);
                            exchange.getResponseBody().write(value);
                        }
                        case "PUT" -> {
                            byte[] newValue = exchange.getRequestBody().readAllBytes();
                            dao.upsert(id, newValue);
                            exchange.sendResponseHeaders(201, -1);
                        }
                        case "DELETE" -> {
                            dao.delete(id);
                            exchange.sendResponseHeaders(202, -1);
                        }
                        default -> exchange.sendResponseHeaders(405, -1);
                    }
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, -1);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
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
                    exchange.sendResponseHeaders(405, -1);
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
            exchange.sendResponseHeaders(500, -1);
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
        }
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }
}
