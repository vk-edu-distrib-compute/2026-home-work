package company.vk.edu.distrib.compute.b10nicle;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean running;

    public KVServiceImpl(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (running) {
            throw new IllegalStateException("Service already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            running = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to start HTTP server", e);
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
            log.error("Error closing DAO", e);
        }
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method not allowed");
                return;
            }

            sendResponse(exchange, 200, "OK");
        }

        private void sendResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
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
                sendResponse(exchange, 400, "Missing id parameter".getBytes(StandardCharsets.UTF_8));
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
                        sendResponse(exchange, 405, "Method not allowed".getBytes(StandardCharsets.UTF_8));
                }
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, e.getMessage().getBytes(StandardCharsets.UTF_8));
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, e.getMessage().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error("IO error handling request", e);
                sendResponse(exchange, 500, "Internal server error".getBytes(StandardCharsets.UTF_8));
            }
        }

        private String extractId(String query) {
            if (query == null || query.isEmpty()) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=");
                if (keyValue.length == 2 && "id".equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            sendResponse(exchange, 200, data);
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            sendResponse(exchange, 201, "Created".getBytes(StandardCharsets.UTF_8));
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            sendResponse(exchange, 202, "Deleted".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] data) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(statusCode, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }
}
