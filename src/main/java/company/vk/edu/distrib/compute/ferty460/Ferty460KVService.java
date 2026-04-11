package company.vk.edu.distrib.compute.ferty460;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class Ferty460KVService implements KVService {

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;

    public Ferty460KVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext("/v0", this::handleRequest);

            server.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        try {
            dao.close();
        } catch (IOException e) {
            // log error
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        try {
            if ("/v0/status".equals(path)) {
                handleStatus(exchange);
            } else if ("/v0/entity".equals(path)) {
                handleEntity(exchange, method);
            } else {
                sendResponse(exchange, 404, "Not Found");
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal Error");
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        // Проверяем, что сервер работает нормально
        sendResponse(exchange, 200, "OK");
    }

    private void handleEntity(HttpExchange exchange, String method) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String id = extractId(query);

        if (id == null || id.isEmpty()) {
            sendResponse(exchange, 400, "Missing id parameter");
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
                sendResponse(exchange, 405, "Method not allowed");
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            sendResponse(exchange, 200, data);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, 404, "Not Found");
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        sendResponse(exchange, 201, "Created");
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendResponse(exchange, 202, "Accepted");
    }

    private String extractId(String query) {
        if (query == null) {
            return null;
        }

        String[] params = query.split("&");
        for (String param : params) {
            String[] pair = param.split("=");
            if (pair.length == 2 && "id".equals(pair[0])) {
                return pair[1];
            }
        }

        return null;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
        exchange.sendResponseHeaders(statusCode, message.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(message.getBytes());
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] data) throws IOException {
        exchange.sendResponseHeaders(statusCode, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

}
