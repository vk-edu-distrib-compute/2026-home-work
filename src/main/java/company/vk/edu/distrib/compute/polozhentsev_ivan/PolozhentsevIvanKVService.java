package company.vk.edu.distrib.compute.polozhentsev_ivan;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class PolozhentsevIvanKVService implements KVService {

    private final HttpServer httpServer;
    private final Dao<byte[]> dao;

    public PolozhentsevIvanKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/v0/status", this::handleStatus);
        httpServer.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(0);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
            sendText(exchange, 405, "Method Not Allowed");
            return;
        }
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            String id = parseId(exchange.getRequestURI().getQuery());
            if (id == null) {
                sendText(exchange, 400, "Bad Request");
                return;
            }
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange, id);
                case "PUT" -> handlePut(exchange, id);
                case "DELETE" -> handleDelete(exchange, id);
                default -> sendText(exchange, 405, "Method Not Allowed");
            }
        } catch (IllegalArgumentException e) {
            sendText(exchange, 400, "Bad Request");
        } catch (NoSuchElementException e) {
            sendText(exchange, 404, "Not Found");
        } catch (RuntimeException e) {
            sendText(exchange, 500, "Internal Server Error");
        }
    }

    private static String parseId(String query) {
        if (query == null || !query.startsWith("id=")) {
            return null;
        }
        String id = query.substring(3);
        if (id.isEmpty()) {
            return null;
        }
        return id;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] data = dao.get(id);
        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(201, 0);
        exchange.close();
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, 0);
        exchange.close();
    }

    private static void sendText(HttpExchange exchange, int code, String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
