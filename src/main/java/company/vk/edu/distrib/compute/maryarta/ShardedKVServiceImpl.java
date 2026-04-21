package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class ShardedKVServiceImpl implements KVService {
    private HttpServer server;
    private final HttpClient client = HttpClient.newHttpClient();
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final int port;
    private boolean started;
    private final ShardingStrategy shardingStrategy;

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
            createContext();
            server.start();
            started = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            return;
        }
        server.stop(0);
        server = null;
        started = false;
    }

    private void createContext() {
        server.createContext("/v0/status", exchange -> {
            String method = exchange.getRequestMethod();
            if ("GET".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        });
        server.createContext("/v0/entity", handleRequest());
    }

    private HttpHandler handleRequest() {
        return exchange -> {
            try {
                String method = exchange.getRequestMethod();
                String query = exchange.getRequestURI().getQuery();
                String target = shardingStrategy.getEndpoint(parseId(query));
                if (!target.equals(selfEndpoint)) {
                    proxyRequest(exchange, target);
                    return;
                }
                switch (method) {
                    case "GET" -> {
                        byte[] value = dao.get(parseId(query));
                        exchange.sendResponseHeaders(200, value.length); // OK
                        exchange.getResponseBody().write(value);
                    }
                    case "PUT" -> {
                        byte[] newValue = exchange.getRequestBody().readAllBytes();
                        dao.upsert(parseId(query), newValue);
                        exchange.sendResponseHeaders(201, 0); // OK
                    }
                    case "DELETE" -> {
                        dao.delete(parseId(query));
                        exchange.sendResponseHeaders(202, 0);
                    }
                    default -> exchange.sendResponseHeaders(405, 0);
                }
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, 0);
            } finally {
                exchange.close();
            }
        };
    }

    private void proxyRequest(HttpExchange exchange, String target) throws IOException {
        try {
            URI uri = URI.create(target + exchange.getRequestURI());
            HttpRequest.Builder request = HttpRequest.newBuilder(uri);
            switch (exchange.getRequestMethod()) {
                case "GET" -> request.GET();
                case "PUT" ->
                        request.PUT(HttpRequest.BodyPublishers
                    .ofByteArray(exchange.getRequestBody().readAllBytes()));
                case "DELETE" -> request.DELETE();
                default -> {
                        exchange.sendResponseHeaders(405, 0);
                    return;
                }
            }
            HttpResponse<byte[]> response = client.send(request.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            byte[] responseBody = response.body();
            if (responseBody == null) {
                responseBody = new byte[0];
            }
                exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
                exchange.getResponseBody().write(responseBody);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, -1);
        }catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
        }
        finally {
            exchange.close();
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
