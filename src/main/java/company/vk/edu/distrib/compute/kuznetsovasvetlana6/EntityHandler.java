package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {
    private final Dao<byte[]> dao;

    public EntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                String id = extractId(exchange);
                if (id == null) {
                    return;
                }
                handleMethod(exchange, id);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (Exception e) {
                exchange.sendResponseHeaders(500, -1);
            }
        } 
    }

    private String extractId(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.startsWith("id=")) {
            exchange.sendResponseHeaders(400, 0);
            return null;
        }
        String id = query.substring(3);
        if (id.isEmpty()) {
            exchange.sendResponseHeaders(400, 0);
            return null;
        }
        return id;
    }

    private void handleMethod(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] data = dao.get(id);
        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] body = is.readAllBytes();
            dao.upsert(id, body);
        }
        exchange.sendResponseHeaders(201, 0);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, 0);
    }
}
