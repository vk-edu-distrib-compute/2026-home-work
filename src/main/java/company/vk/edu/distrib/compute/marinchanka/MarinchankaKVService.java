package company.vk.edu.distrib.compute.marinchanka;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class MarinchankaKVService implements KVService {
    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean running;

    public MarinchankaKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (running) {
            throw new IllegalStateException("Server already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());

            server.setExecutor(null);
            server.start();
            running = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server", e);
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
        }
        try {
            dao.close();
        } catch (IOException e) {
            // Log error silently
        }
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            if (running) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(503, -1);
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
                sendError(exchange, 400, "Missing id parameter");
                return;
            }

            try {
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
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, 400, e.getMessage());
            } catch (NoSuchElementException e) {
                sendError(exchange, 404, e.getMessage());
            } catch (IOException e) {
                sendError(exchange, 500, "Internal server error");
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(201, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            exchange.sendResponseHeaders(202, -1);
        }

        private String extractId(String query) {
            if (query == null) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && "id".equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void sendError(HttpExchange exchange, int code, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }
}
