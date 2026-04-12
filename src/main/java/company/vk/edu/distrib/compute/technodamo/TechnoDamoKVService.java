package company.vk.edu.distrib.compute.technodamo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class TechnoDamoKVService implements KVService {
    private static final int NO_RESPONSE_BODY = -1;

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public TechnoDamoKVService(int port, Dao<byte[]> dao) throws IOException {
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
        try {
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleStatus(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, NO_RESPONSE_BODY);
                return;
            }
            exchange.sendResponseHeaders(200, NO_RESPONSE_BODY);
        } finally {
            exchange.close();
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            String id = extractId(exchange);
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange, id);
                case "PUT" -> handlePut(exchange, id);
                case "DELETE" -> handleDelete(exchange, id);
                default -> exchange.sendResponseHeaders(405, NO_RESPONSE_BODY);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, NO_RESPONSE_BODY);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(400, NO_RESPONSE_BODY);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, NO_RESPONSE_BODY);
        } finally {
            exchange.close();
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(value);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(201, NO_RESPONSE_BODY);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, NO_RESPONSE_BODY);
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing id");
        }
        for (String parameter : query.split("&")) {
            int separatorIndex = parameter.indexOf('=');
            String name;
            String rawValue;
            if (separatorIndex < 0) {
                name = parameter;
                rawValue = "";
            } else {
                name = parameter.substring(0, separatorIndex);
                rawValue = parameter.substring(separatorIndex + 1);
            }
            if ("id".equals(name)) {
                String value = URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
                if (value.isEmpty()) {
                    throw new IllegalArgumentException("Empty id");
                }
                return value;
            }
        }
        throw new IllegalArgumentException("Missing id");
    }
}
