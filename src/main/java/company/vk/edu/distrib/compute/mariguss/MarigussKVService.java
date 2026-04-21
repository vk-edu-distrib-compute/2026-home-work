package company.vk.edu.distrib.compute.mariguss;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class MarigussKVService implements KVService {
    private HttpServer server;
    private final int port;
    private final MarigussDao dao;
    private final HttpClient httpClient;
    private final String selfEndpoint;
    private List<String> clusterEndpoints;
    private boolean isRunning;

    public MarigussKVService(int port) throws IOException {
        this.port = port;
        this.dao = new MarigussDao();
        this.httpClient = HttpClient.newHttpClient();
        this.selfEndpoint = "http://localhost:" + port;
        this.clusterEndpoints = new ArrayList<>();
        this.clusterEndpoints.add(this.selfEndpoint);
    }

    public void setClusterEndpoints(List<String> endpoints) {
        this.clusterEndpoints = new ArrayList<>(endpoints);
    }

    @Override
    public synchronized void start() {
        if (isRunning) {
            return;
        }
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", this::handleStatus);
            server.createContext("/v0/entity", this::handleEntity);
            server.setExecutor(null);
            server.start();
            isRunning = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public synchronized void stop() {
        if (!isRunning) {
            return;
        }
        server.stop(0);
        isRunning = false;
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) { 
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String key = extractKey(query);

            if (key == null || key.isEmpty()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            String targetEndpoint = MarigussHashUtils.getNodeEndpoint(key, clusterEndpoints);

            if (!targetEndpoint.equals(selfEndpoint)) {
                if (exchange.getRequestHeaders().containsKey("X-Proxy")) {
                    exchange.sendResponseHeaders(508, -1);
                    return;
                }
                proxyRequest(exchange, targetEndpoint);
                return;
            }

            processLocal(exchange, method, key);

        } catch (Exception e) {
            try {
                exchange.sendResponseHeaders(500, -1);
            } catch (IOException ignored) {
                // Если отправка ответа не удалась, просто игнорируем исключение, 
                // так как мы уже в процессе обработки другого исключения
            }
        }
    }

    private void processLocal(HttpExchange exchange, String method, String key) throws IOException {
        switch (method) {
            case "GET":
                handleGet(exchange, key);
                break;
            case "PUT":
                handlePut(exchange, key);
                break;
            case "DELETE":
                handleDelete(exchange, key);
                break;
            default:
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                break;
        }
    }

    private void proxyRequest(HttpExchange exchange, String targetEndpoint) throws IOException {
        try {
            String uri = targetEndpoint + exchange.getRequestURI().toString();
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(uri))
                    .header("X-Proxy", "true");

            String method = exchange.getRequestMethod();
            if ("PUT".equals(method)) {
                byte[] body;
                // Ресурс (InputStream) должен быть объявлен здесь:
                try (java.io.InputStream is = exchange.getRequestBody()) {
                    body = is.readAllBytes();
                }
                requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            } else if ("DELETE".equals(method)) {
                requestBuilder.DELETE();
            } else {
                requestBuilder.GET();
            }

            HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(), 
                    HttpResponse.BodyHandlers.ofByteArray());
            byte[] responseBody = response.body();

            if (responseBody != null && responseBody.length > 0) {
                exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBody);
                }
            } else {
                exchange.sendResponseHeaders(response.statusCode(), -1);
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(503, -1);
        }
    }

    private void handlePut(HttpExchange exchange, String key) throws IOException {
        try {
            byte[] body;
            // И здесь тоже:
            try (java.io.InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }
            dao.upsert(key, body);
            exchange.sendResponseHeaders(201, -1);
        } catch (Exception e) {
            exchange.sendResponseHeaders(400, -1);
        }
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        try {
            byte[] value = dao.get(key);
            exchange.sendResponseHeaders(200, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        try {
            dao.delete(key);
            exchange.sendResponseHeaders(202, -1);
        } catch (Exception e) {
            exchange.sendResponseHeaders(400, -1);
        }
    }

    private String extractKey(String query) {
        // 4. Фигурные скобки и LiteralsFirst (ControlStatementBraces)
        if (query == null || !query.startsWith("id=")) {
            return null;
        }
        String key = query.substring(3);
        return key.isEmpty() ? null : key;
    }
}
