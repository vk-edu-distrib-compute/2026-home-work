package company.vk.edu.distrib.compute.lillymega;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;

public class LillymegaKVService implements KVService {
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String ID_PARAMETER = "id";
    private static final int QUERY_PARTS_COUNT = 2;
    private static final String INTERNAL_HEADER = "X-Cluster-Internal";
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final List<String> clusterEndpoints;
    private final HttpClient httpClient;

    public LillymegaKVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, null, List.of(), HttpClient.newHttpClient());
    }

    public LillymegaKVService(
            int port,
            Dao<byte[]> dao,
            String selfEndpoint,
            List<String> clusterEndpoints,
            HttpClient httpClient
    ) throws IOException {
        this.dao = dao;
        this.selfEndpoint = selfEndpoint;
        this.clusterEndpoints = List.copyOf(clusterEndpoints);
        this.httpClient = httpClient;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = extractId(exchange);

        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }
        if (shouldProxy(exchange, id)) {
            proxyRequest(exchange, selectEndpoint(id));
            return;
        }

        String method = exchange.getRequestMethod();

        try {
            switch (method) {
                case METHOD_GET -> handleGet(exchange, id);
                case METHOD_PUT -> handlePut(exchange, id);
                case METHOD_DELETE -> handleDelete(exchange, id);
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                }
            }
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        }
    }

    private boolean shouldProxy(HttpExchange exchange, String id) {
        if (selfEndpoint == null || clusterEndpoints.isEmpty()) {
            return false;
        }

        if (exchange.getRequestHeaders().containsKey(INTERNAL_HEADER)) {
            return false;
        }

        return !selfEndpoint.equals(selectEndpoint(id));
    }

    private String selectEndpoint(String id) {
        String selectedEndpoint = selfEndpoint;
        long maxHash = Long.MIN_VALUE;
        for (String endpoint : clusterEndpoints) {
            long hash = rendezvousHash(id, endpoint);
            if (hash > maxHash) {
                maxHash = hash;
                selectedEndpoint = endpoint;
            }
        }

        return selectedEndpoint;
    }

    private long rendezvousHash(String key, String endpoint) {
        long hash = 1469598103934665603L;
        String combined = key + '#' + endpoint;
        for (int i = 0; i < combined.length(); i++) {
            hash ^= combined.charAt(i);
            hash *= 1099511628211L;
        }
        return hash;
    }

    private void proxyRequest(HttpExchange exchange, String targetEndpoint) throws IOException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(new URI(targetEndpoint + exchange.getRequestURI()))
                    .timeout(PROXY_TIMEOUT)
                    .header(INTERNAL_HEADER, "true");

            switch (exchange.getRequestMethod()) {
                case METHOD_GET -> builder.GET();
                case METHOD_DELETE -> builder.DELETE();
                case METHOD_PUT -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()));
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                    return;
                }
            }

            HttpResponse<byte[]> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            sendProxyResponse(exchange, response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        } catch (URISyntaxException e) {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        }
    }

    private void sendProxyResponse(HttpExchange exchange, HttpResponse<byte[]> response) throws IOException {
        byte[] body = response.body();
        if (body == null || body.length == 0) {
            exchange.sendResponseHeaders(response.statusCode(), -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(response.statusCode(), body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(201, -1);
        exchange.close();
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
        exchange.close();
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        exchange.getResponseBody().write(value);
        exchange.close();
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }

        String[] parts = query.split("=", QUERY_PARTS_COUNT);
        if (parts.length != QUERY_PARTS_COUNT) {
            return null;
        }

        if (!ID_PARAMETER.equals(parts[0])) {
            return null;
        }

        return parts[1];
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
