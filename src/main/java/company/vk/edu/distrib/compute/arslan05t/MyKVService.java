package company.vk.edu.distrib.compute.arslan05t;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class MyKVService implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public MyKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
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
            if ("GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 200, "OK".getBytes());
            } else {
                sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            Map<String, String> params = parseQuery(exchange.getRequestURI());
            String id = params.get("id");

            if (id == null || id.isEmpty()) {
                sendResponse(exchange, 400, "Bad Request".getBytes());
                return;
            }

            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(id, exchange);
                case "PUT" -> handlePut(id, exchange);
                case "DELETE" -> handleDelete(id, exchange);
                default -> sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            }
        }
    }

    private void handleGet(String id, HttpExchange exchange) throws IOException {
        try {
            byte[] value = dao.get(id);
            sendResponse(exchange, 200, value);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, 404, "Not Found".getBytes());
        }
    }

    private void handlePut(String id, HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            dao.upsert(id, is.readAllBytes());
        }
        sendResponse(exchange, 201, "Created".getBytes());
    }

    private void handleDelete(String id, HttpExchange exchange) throws IOException {
        dao.delete(id);
        sendResponse(exchange, 202, "Accepted".getBytes());
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] response) throws IOException {
        exchange.sendResponseHeaders(code, response.length == 0 ? -1 : response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private Map<String, String> parseQuery(URI uri) {
        if (uri.getQuery() == null || uri.getQuery().isEmpty()) {
            return Map.of();
        }
        return Arrays.stream(uri.getQuery().split("&"))
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(arr -> arr[0], arr -> arr.length > 1 ? arr[1] : ""));
    }
}
