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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NoSuchElementException;

public class ShardedKVServiceImpl implements KVService {
    private HttpServer server;
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final List<String> endpoints;
    private boolean started;
    private final int port;
    private final HttpClient client = HttpClient.newHttpClient();

    public ShardedKVServiceImpl(int port, List<String> endpoints) throws IOException {
        this.port = port;
        this.dao = new H2Dao("node-" + port);
        this.selfEndpoint = "http://localhost:" + port;
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
        server.createContext("/v0/status", exchange -> {
            String method = exchange.getRequestMethod();
            if ("GET".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        });
        System.out.println(this.port + "adas");
        server.createContext("/v0/entity", handleRequest());
    }

    private HttpHandler handleRequest(){
        return exchange -> {
            try {
                boolean internal = exchange.getRequestHeaders().containsKey("internal-request");
                String method = exchange.getRequestMethod();
                String query = exchange.getRequestURI().getQuery();
                String target = resolve(parseId(query));
                if (!internal && !target.equals(selfEndpoint)) {
                    System.out.println("other target: " + selfEndpoint);
                    proxyRequest(exchange, target);
                    return;
                }else {
                    System.out.println("self target: " + target);
                }
                switch (method) {
                    case "GET" -> {
//                        System.out.println("get " + target);
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
            }
            finally {
                exchange.close();
            }
        };
    }

    private void proxyRequest(HttpExchange http, String target) throws IOException {
        try {
            URI uri = URI.create(target + http.getRequestURI());
            HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                    .header("internal-request", "true");
            switch (http.getRequestMethod()) {
                case "GET" -> request.GET();
                case "PUT" -> request.PUT(HttpRequest.BodyPublishers.ofByteArray(http.getRequestBody().readAllBytes()));
                case "DELETE" -> request.DELETE();
                default -> {
                    http.sendResponseHeaders(405, 0);
                    return;
                }
            }

            HttpResponse<byte[]> response = client.send(request.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            byte[] responseBody = response.body();
            if (responseBody == null) {
                responseBody = new byte[0];
            }
            http.sendResponseHeaders(response.statusCode(), responseBody.length);
            http.getResponseBody().write(responseBody);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            http.sendResponseHeaders(500, -1);
        }catch (IOException e) {
            http.sendResponseHeaders(503, -1);
        }
        finally {
            http.close();
        }
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    private String resolve(String key) {
        long max = Long.MIN_VALUE;
        String target = "";
        for(String endpoint: endpoints){
            long score = score(key, endpoint);
            if(score > max){
                max = score;
                target = endpoint;
            }
        }
        return target;
    }

    private long score (String key, String endpoint) {
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
}
