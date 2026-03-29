package company.vk.edu.distrib.compute.igor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class KVServiceImpl implements KVService {
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private final int port;
    private final Dao<byte[]> dao;
    private final HttpServer server;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.port = port;
        this.dao = dao;
        this.server = HttpServer.create();

        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        try {
            server.bind(newListenAddress(port), 0);
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not start HTTP server", e);
        }
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not close dao", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            handleEntityRequest(exchange);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(400, -1);
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
        }
    }

    private void handleEntityRequest(HttpExchange exchange) throws IOException {
        String id = idFromQuery(exchange.getRequestURI().getRawQuery());
        String method = exchange.getRequestMethod();

        switch (method) {
            case GET_METHOD:
                byte[] value = dao.get(id);
                exchange.sendResponseHeaders(200, value.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(value);
                }
                return;
            case PUT_METHOD:
                byte[] requestBody = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, requestBody);
                exchange.sendResponseHeaders(201, -1);
                return;
            default:
                exchange.sendResponseHeaders(405, -1);
        }
    }

    private static String idFromQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            throw new IllegalArgumentException("Missing query params");
        }
        Map<String, String> queryParams = parseQuery(rawQuery);

        return queryParams.get("id");
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> params = new ConcurrentHashMap<>();
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";
            params.put(key, value);
        }
        return params;
    }

    private static InetSocketAddress newListenAddress(int port) {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }
}
