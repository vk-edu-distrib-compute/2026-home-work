package company.vk.edu.distrib.compute.marinchanka;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

public class MarinchankaKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(MarinchankaKVService.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String PARAM_ID = "id";
    private static final String CONTENT_TYPE_VALUE = "application/octet-stream";

    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int INTERNAL_ERROR = 500;
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int SERVICE_UNAVAILABLE = 503;

    private static final java.net.http.HttpClient PROXY_CLIENT = java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final int port;
    private final Dao<byte[]> dao;
    private final ConsistentHashingRouter router;
    private HttpServer server;
    private boolean running;

    public MarinchankaKVService(int port, Dao<byte[]> dao) {
        this(port, dao, null);
    }

    public MarinchankaKVService(int port, Dao<byte[]> dao, ConsistentHashingRouter router) {
        this.port = port;
        this.dao = dao;
        this.router = router;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(null);
            server.start();
            running = true;
            log.info("KVService started on port {}", port);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        if (server != null) {
            server.stop(0);
            server = null;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error closing DAO", e);
        }
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                return;
            }

            if (running) {
                exchange.sendResponseHeaders(STATUS_OK, -1);
            } else {
                exchange.sendResponseHeaders(SERVICE_UNAVAILABLE, -1);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);

            if (id == null) {
                sendError(exchange, BAD_REQUEST, "Missing id parameter");
                return;
            }

            if (router != null) {
                ClusterNode responsibleNode = router.getNode(id);
                if (responsibleNode.port() != port) {
                    proxyRequest(exchange, responsibleNode, id);
                    return;
                }
            }

            try {
                switch (method) {
                    case METHOD_GET:
                        handleGet(exchange, id);
                        break;
                    case METHOD_PUT:
                        handlePut(exchange, id);
                        break;
                    case METHOD_DELETE:
                        handleDelete(exchange, id);
                        break;
                    default:
                        exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                        break;
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, BAD_REQUEST, e.getMessage());
            } catch (NoSuchElementException e) {
                sendError(exchange, NOT_FOUND, e.getMessage());
            } catch (IOException e) {
                log.error("Internal server error", e);
                sendError(exchange, INTERNAL_ERROR, "Internal server error");
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_VALUE);
            exchange.sendResponseHeaders(STATUS_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(STATUS_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            exchange.sendResponseHeaders(STATUS_ACCEPTED, -1);
        }

        private void proxyRequest(HttpExchange exchange, ClusterNode targetNode, String id) throws IOException {
            try {
                String targetUrl = targetNode.getUrl() + "/v0/entity?id=" + id;
                String method = exchange.getRequestMethod();

                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .timeout(Duration.ofSeconds(5));

                HttpResponse<byte[]> response;

                switch (method) {
                    case METHOD_GET:
                        requestBuilder.GET();
                        response = PROXY_CLIENT.send(requestBuilder.build(),
                                HttpResponse.BodyHandlers.ofByteArray());
                        break;
                    case METHOD_PUT:
                        byte[] body = exchange.getRequestBody().readAllBytes();
                        requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                        response = PROXY_CLIENT.send(requestBuilder.build(),
                                HttpResponse.BodyHandlers.ofByteArray());
                        break;
                    case METHOD_DELETE:
                        requestBuilder.DELETE();
                        response = PROXY_CLIENT.send(requestBuilder.build(),
                                HttpResponse.BodyHandlers.ofByteArray());
                        break;
                    default:
                        exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                        return;
                }

                response.headers().map().forEach((key, values) -> {
                    if (!"Content-Length".equalsIgnoreCase(key)
                            && !"Transfer-Encoding".equalsIgnoreCase(key)) {
                        exchange.getResponseHeaders().put(key, values);
                    }
                });

                byte[] responseBody = response.body();
                if (responseBody != null && responseBody.length > 0) {
                    exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(responseBody);
                    }
                } else {
                    exchange.sendResponseHeaders(response.statusCode(), -1);
                }

            } catch (ConnectException e) {
                sendError(exchange, NOT_FOUND, "Node unavailable");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendError(exchange, INTERNAL_ERROR, "Proxy interrupted");
            } catch (Exception e) {
                log.error("Proxy error", e);
                sendError(exchange, INTERNAL_ERROR, "Proxy error: " + e.getMessage());
            }
        }

        private String extractId(String query) {
            if (query == null) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && PARAM_ID.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void sendError(HttpExchange exchange, int code, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(code, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }

    public Dao<byte[]> getDao() {
        return dao;
    }
}
