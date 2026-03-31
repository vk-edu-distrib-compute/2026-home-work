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
            String query = exchange.getRequestURI().getQuery();
            if (query == null || !query.startsWith("id=")) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            String id = query.substring(3);
            if (id.isEmpty()) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            String method = exchange.getRequestMethod();

            try {
                switch (method) {
                    case "GET" -> {
                        byte[] data = dao.get(id);
                        exchange.sendResponseHeaders(200, data.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(data);
                        }
                    }
                    case "PUT" -> {
                        try (InputStream is = exchange.getRequestBody()) {
                            byte[] body = is.readAllBytes();
                            dao.upsert(id, body);
                        }
                        exchange.sendResponseHeaders(201, 0);
                    }
                    case "DELETE" -> {
                        dao.delete(id);
                        exchange.sendResponseHeaders(202, 0);
                    }
                    default -> exchange.sendResponseHeaders(405, 0);
                }
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, 0);
            }
        }
    }
}
