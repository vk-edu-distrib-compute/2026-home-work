package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class TadzhnahalKVService implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean started;
    private boolean stopped;

    public TadzhnahalKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (started && !stopped) {
            throw new IllegalStateException("Server already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(STATUS_PATH, this::handleStatus);
            server.createContext(ENTITY_PATH, this::handleEntity);
            server.start();
            started = true;
            stopped = false;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void stop() {
        if (!started || stopped) {
            throw new IllegalStateException("Server is not started or already stopped");
        }

        server.stop(0);
        started = false;
        stopped = true;

        try {
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot close dao", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, 404);
                return;
            }

            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, 405);
                return;
            }

            sendEmptyResponse(exchange, 200);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                    sendEmptyResponse(exchange, 404);
                    return;
                }

                String id = extractId(exchange);

                if (isInternalRequest(exchange)) {
                    handleInternalEntity(exchange, id);
                    return;
                }

                handleClientEntity(exchange, id);
            } catch (IllegalArgumentException e) {
                sendEmptyResponse(exchange, 400);
            } catch (NoSuchElementException e) {
                sendEmptyResponse(exchange, 404);
            } catch (IOException e) {
                sendEmptyResponse(exchange, 500);
            }
        }
    }

    private void handleClientEntity(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method)) {
            handleLocalGet(exchange, id);
            return;
        }

        if (METHOD_PUT.equals(method)) {
            handleLocalPut(exchange, id);
            return;
        }

        if (METHOD_DELETE.equals(method)) {
            handleLocalDelete(exchange, id);
            return;
        }

        sendEmptyResponse(exchange, 405);
    }

    private void handleInternalEntity(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method)) {
            handleLocalGet(exchange, id);
            return;
        }

        if (METHOD_PUT.equals(method)) {
            handleLocalPut(exchange, id);
            return;
        }

        if (METHOD_DELETE.equals(method)) {
            handleLocalDelete(exchange, id);
            return;
        }

        sendEmptyResponse(exchange, 405);
    }

    private void handleLocalGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        exchange.getResponseBody().write(value);
    }

    private void handleLocalPut(HttpExchange exchange, String id) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);
        sendEmptyResponse(exchange, 201);
    }

    private void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendEmptyResponse(exchange, 202);
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        String headerValue = exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        return INTERNAL_REQUEST_VALUE.equalsIgnoreCase(headerValue);
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();

        if (query == null) {
            throw new IllegalArgumentException("Missing query");
        }

        if (!query.startsWith("id=")) {
            throw new IllegalArgumentException("Missing id parameter");
        }

        if (query.contains("&")) {
            throw new IllegalArgumentException("Unexpected parameters");
        }

        String id = query.substring("id=".length());

        if (id.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }

        return id;
    }

    private void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }
}
