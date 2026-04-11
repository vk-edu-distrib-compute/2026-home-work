package company.vk.edu.distrib.compute.ronshinvsevolod;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class ShardedKVService implements KVService {
    private HttpServer server;
    private final Dao<byte[]> dao;
    private final int port;
    private final String myEndpoint;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final HashStrategy hashStrategy;
    private static final int EXPECTED_PARTS = 2;
    private static final String ID_PARAM = "id";
    private volatile boolean running;

    public ShardedKVService(Dao<byte[]> dao, int port, HashStrategy hashStrategy) {
        this.dao = dao;
        this.port = port;
        this.myEndpoint = "http://localhost:" + port;
        this.hashStrategy = hashStrategy;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @Override
    public void start() {
        if (running) {
            throw new IllegalStateException("Already started");
        }
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            running = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (running) {
            server.stop(0);
            running = false;
        }
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }

            String owner = hashStrategy.getEndpoint(id);
            if (!owner.equals(myEndpoint)) {
                proxyRequest(exchange, owner, method, id);
                return;
            }

            switch (method) {
                case "GET":
                    handleGet(exchange, id);
                    break;
                case "PUT":
                    handlePut(exchange, id);
                    break;
                case "DELETE":
                    handleDelete(exchange, id);
                    break;
                default:
                    exchange.sendResponseHeaders(405, -1);
                    break;
            }
            exchange.close();
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            try {
                byte[] data = dao.get(id);
                exchange.sendResponseHeaders(200, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                exchange.sendResponseHeaders(201, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            try {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }
        
        // HttpExchange не AutoCloseable и я не вижу смысла это обходить
        @SuppressWarnings("PMD.UseTryWithResources")
        private void proxyRequest(HttpExchange exchange, String target, String method, String id) throws IOException {
            try {
                String url = target + "/v0/entity?id=" + id;
                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5));
                if ("GET".equals(method)) {
                    builder.GET();
                } else if ("PUT".equals(method)) {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                } else if ("DELETE".equals(method)) {
                    builder.DELETE();
                } else {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                    return;
                }
                HttpResponse<byte[]> response = httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
                );
                exchange.sendResponseHeaders(response.statusCode(), response.body().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.body());
                }
            } catch (InterruptedException | IOException e) {
                exchange.sendResponseHeaders(404, -1);
            } finally {
                exchange.close();
            }
        }

        private String extractId(String query) {
            if (query == null) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == EXPECTED_PARTS && ID_PARAM.equals(pair[0])) {
                    return pair[1];
                }
            }
            return null;
        }
    }
}
