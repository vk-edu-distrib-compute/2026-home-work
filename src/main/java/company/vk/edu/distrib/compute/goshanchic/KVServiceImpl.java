package company.vk.edu.distrib.compute.goshanchic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLSession;

public class KVServiceImpl implements ReplicatedService {
    @SuppressWarnings("HttpUrlsUsage")
    private static final String PROXIED_HEADER = "X-Internal-Request";

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
    private final ExecutorService executor;

    private final int replicationFactor;
    private int defaultAck;

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao,
                         int replicationFactor, int defaultAck) throws IOException {

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.selfAddress = "http://localhost:" + port;

        this.clusterNodes = allPorts.stream()
                .map(p -> "http://localhost:" + p)
                .toList();

        this.httpClient = HttpClient.newHttpClient();
        this.executor = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors())
        );

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
        this.defaultAck = ack;
    }

    private void setupEndpoints() {
        server.createContext("/v0/entity", exchange -> {
            try {
                handleRequest(exchange);
            } catch (Exception e) {
                sendResponse(exchange, STATUS_INTERNAL_ERROR, null);
            }
        });

        server.createContext("/v0/status", exchange ->
                sendResponse(exchange, STATUS_OK, "OK".getBytes()));
    }

    private void handleRequest(HttpExchange exchange) throws Exception {
        String id = extractId(exchange);

        if (id == null || id.isEmpty()) {
            sendResponse(exchange, STATUS_BAD_REQUEST, null);
            return;
        }

        boolean isInternal = exchange.getRequestHeaders().containsKey(PROXIED_HEADER);

        if (isInternal) {
            handleLocal(exchange, id);
        } else {
            handleDistributed(exchange, id);
        }
    }

    // ================= LOCAL =================

    private void handleLocal(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> {
                try {
                    byte[] value = dao.get(id);
                    sendResponse(exchange, STATUS_OK, value);
                } catch (NoSuchElementException e) {
                    sendResponse(exchange, STATUS_NOT_FOUND, null);
                }
            }
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                sendResponse(exchange, STATUS_CREATED, null);
            }
            case "DELETE" -> {
                dao.delete(id);
                sendResponse(exchange, STATUS_ACCEPTED, null);
            }
            default -> sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, null);
        }
    }

    // ================= DISTRIBUTED =================

    private void handleDistributed(HttpExchange exchange, String id) throws Exception {
        int ack = extractAck(exchange);
        List<String> replicas = getReplicas(id);
        byte[] body = exchange.getRequestBody().readAllBytes();

        List<CompletableFuture<HttpResponse<byte[]>>> futures = replicas.stream()
                .map(node -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return send(node, exchange.getRequestMethod(), id, body);
                    } catch (Exception e) {
                        return fakeResponse(STATUS_SERVICE_UNAVAILABLE, null);
                    }
                }, executor))
                .toList();

        int success = 0;
        byte[] result = null;

        for (CompletableFuture<HttpResponse<byte[]>> future : futures) {
            try {
                HttpResponse<byte[]> response = future.get();
                if (response.statusCode() < 500) {
                    success++;
                    if (exchange.getRequestMethod().equals("GET")
                            && response.statusCode() == STATUS_OK) {
                        result = response.body();
                    }
                }
            } catch (Exception e) {
                // Пропускаем ошибки
            }
        }

        if (success < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE, null);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> {
                if (result == null) {
                    sendResponse(exchange, STATUS_NOT_FOUND, null);
                } else {
                    sendResponse(exchange, STATUS_OK, result);
                }
            }
            case "PUT" -> sendResponse(exchange, STATUS_CREATED, null);
            case "DELETE" -> sendResponse(exchange, STATUS_ACCEPTED, null);
        }
    }

    // ================= RENDEZVOUS HASHING =================

    private List<String> getReplicas(String key) {
        return clusterNodes.stream()
                .sorted((a, b) -> Long.compare(
                        hash(key, b),
                        hash(key, a)
                ))
                .limit(replicationFactor)
                .toList();
    }

    private long hash(String key, String node) {
        String combined = key + node;
        return combined.hashCode() & 0xffffffffL;
    }

    // ================= NETWORK =================

    private HttpResponse<byte[]> send(String node, String method, String id, byte[] body)
            throws Exception {

        if (node.equals(selfAddress)) {
            return handleSelf(method, id, body);
        }

        URI uri = URI.create(node + "/v0/entity?id=" + id);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .header(PROXIED_HEADER, "true")
                .timeout(java.time.Duration.ofSeconds(5));

        switch (method) {
            case "GET" -> builder.GET();
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            case "DELETE" -> builder.DELETE();
        }

        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<byte[]> handleSelf(String method, String id, byte[] body) throws Exception {
        switch (method) {
            case "GET" -> {
                try {
                    return fakeResponse(STATUS_OK, dao.get(id));
                } catch (NoSuchElementException e) {
                    return fakeResponse(STATUS_NOT_FOUND, null);
                }
            }
            case "PUT" -> {
                dao.upsert(id, body);
                return fakeResponse(STATUS_CREATED, null);
            }
            case "DELETE" -> {
                dao.delete(id);
                return fakeResponse(STATUS_ACCEPTED, null);
            }
        }
        return fakeResponse(STATUS_INTERNAL_ERROR, null);
    }

    private HttpResponse<byte[]> fakeResponse(int code, byte[] body) {
        return new HttpResponse<>() {
            @Override public int statusCode() { return code; }
            @Override public byte[] body() { return body; }
            @Override public HttpRequest request() { return null; }
            @Override public Optional<HttpResponse<byte[]>> previousResponse() { return Optional.empty(); }
            @Override public HttpHeaders headers() {
                return HttpHeaders.of(Map.of(), (k, v) -> true);
            }
            @Override public URI uri() { return null; }
            @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
            @Override public Optional<SSLSession> sslSession() { return Optional.empty(); }
        };
    }

    // ================= UTILS =================

    private int extractAck(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) return defaultAck;

        for (String p : query.split("&")) {
            String[] kv = p.split("=");
            if (kv.length == 2 && kv[0].equals("ack")) {
                return Integer.parseInt(kv[1]);
            }
        }
        return defaultAck;
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) return null;

        for (String p : query.split("&")) {
            String[] kv = p.split("=");
            if (kv.length == 2 && kv[0].equals("id")) {
                return kv[1];
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body == null ? -1 : body.length);
        if (body != null) {
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
        executor.shutdown();
    }
}








