package company.vk.edu.distrib.compute.d1gitale;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(200, 0);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
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
                    exchange.sendResponseHeaders(405, 0);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);
            exchange.sendResponseHeaders(200, value.length);
            exchange.getResponseBody().write(value);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, 0);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);
        exchange.sendResponseHeaders(201, 0);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, 0);
    }
}
