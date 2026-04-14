package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NoSuchElementException;

public class ShardedKVServiceImpl implements KVService {
    private HttpServer server;
    private final Dao<byte[]> dao;
    private final String endpoint;
    private final List<String> endpoints;
    boolean started;
    private final int port;

    public ShardedKVServiceImpl(int port, String endpoint, List<String> endpoints) throws IOException {
        this.port = port;
        this.dao = new H2Dao("node-" + port); // пример
        this.endpoint = endpoint;
        this.endpoints = List.copyOf(endpoints);
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
        // определить, кому принадлежит запрос,
        server.createContext("/v0/status", http -> {
            String method = http.getRequestMethod();
            if ("GET".equals(method)) {
                http.sendResponseHeaders(200,-1);
            } else {
                http.sendResponseHeaders(405,-1);
            }
            http.close();
        });
        server.createContext("/v0/entity", http -> {
            try {
                boolean internal = http.getRequestHeaders().containsKey("X-Internal-Request");
            String method = http.getRequestMethod();
            String query = http.getRequestURI().getQuery();
            String target = resolve(parseId(query));
            if (!internal && !target.equals(endpoint)) {
                proxyRequest(http, target);
                return;
            }

                switch (method) {
                    case "GET" -> {
                        byte [] value = dao.get(parseId(query));
                        http.sendResponseHeaders(200, value.length); // OK
                        http.getResponseBody().write(value);
                    }
                    case "PUT" -> {
                        byte[] newValue = http.getRequestBody().readAllBytes();
                        dao.upsert(parseId(query), newValue);
                        http.sendResponseHeaders(201, 0); // OK
                    }
                    case "DELETE" -> {
                        dao.delete(parseId(query));
                        http.sendResponseHeaders(202, 0);
                    }
                    default -> http.sendResponseHeaders(405, 0);
                }
            } catch (IllegalArgumentException e) {
                http.sendResponseHeaders(400, 0); // Bad Request
            } catch (NoSuchElementException e) {
                http.sendResponseHeaders(404, 0); // Not Found
            }
            http.close();
        });
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    private String resolve(String key){
        long max = Long.MIN_VALUE;
        String end = "";
        for(String s: endpoints){
            long score = score(key, s);
            if(score > max){
                max = score;
                end = s;
            }
        }
        return end;
    }

    private long score (String key, String endpoint){
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest((key + endpoint).getBytes());
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 0xffL);
            }
            return value;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

    private final HttpClient client = HttpClient.newHttpClient();

    private void proxyRequest(HttpExchange exchange, String target) throws IOException {
        try {
            URI uri = URI.create(target + exchange.getRequestURI());

            byte[] requestBody = exchange.getRequestBody().readAllBytes();

            HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                    .header("X-Internal-Request", "true");

            String method = exchange.getRequestMethod();
            switch (method) {
                case "GET" -> builder.GET();
                case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(requestBody));
                case "DELETE" -> builder.DELETE();
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
            }

            HttpResponse<byte[]> response = client.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            byte[] responseBody = response.body();
            if (responseBody == null) {
                responseBody = new byte[0];
            }

            exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, -1);
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
        }
    }
}
