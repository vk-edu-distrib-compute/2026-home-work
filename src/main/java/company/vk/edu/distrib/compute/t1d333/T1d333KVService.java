package company.vk.edu.distrib.compute.t1d333;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class T1d333KVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(T1d333KVService.class);
    private static final String API_PREFIX = "/v0";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final Path storageDir;
    private final int port;

    public T1d333KVService(int port) throws IOException {
        this.port = port;
        this.storageDir = resolveStorageDir(port);
        Files.createDirectories(storageDir);
        this.dao = new FileDao(storageDir);
        this.server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        setupRoutes();
    }

    private static Path resolveStorageDir(int port) {
        return Path.of(".t1d333-data", "port-" + port).toAbsolutePath().normalize();
    }

    private void setupRoutes() {
        server.createContext(API_PREFIX + "/status", exchange -> {
            try {
                handleStatus(exchange);
            } catch (Exception e) {
                log.error("Error in status handler", e);
                sendError(exchange, 500);
            }
        });

        server.createContext(API_PREFIX + "/entity", exchange -> {
            try {
                String method = exchange.getRequestMethod();
                switch (method) {
                    case METHOD_GET -> handleGet(exchange);
                    case METHOD_PUT -> handlePut(exchange);
                    case METHOD_DELETE -> handleDelete(exchange);
                    default -> sendError(exchange, 405);
                }
            } catch (Exception e) {
                log.error("Error in entity handler", e);
                sendError(exchange, 500);
            }
        });
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            sendError(exchange, 405);
            return;
        }
        sendResponse(exchange, 200, new byte[0]);
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        String id = getQueryParam(exchange.getRequestURI(), "id");

        if (id == null || id.isEmpty()) {
            sendError(exchange, 400);
            return;
        }

        try {
            byte[] value = dao.get(id);
            sendResponse(exchange, 200, value);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, 404, new byte[0]);
        }
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        String id = getQueryParam(exchange.getRequestURI(), "id");

        if (id == null || id.isEmpty()) {
            sendError(exchange, 400);
            return;
        }

        try (var requestBody = exchange.getRequestBody()) {
            byte[] body = requestBody.readAllBytes();
            dao.upsert(id, body);
        }
        sendResponse(exchange, 201, new byte[0]);
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        String id = getQueryParam(exchange.getRequestURI(), "id");

        if (id == null || id.isEmpty()) {
            sendError(exchange, 400);
            return;
        }

        dao.delete(id);
        sendResponse(exchange, 202, new byte[0]);
    }

    private String getQueryParam(URI uri, String paramName) {
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2 && pair[0].equals(paramName)) {
                return URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        long contentLength = body.length == 0 ? -1 : body.length;
        exchange.sendResponseHeaders(statusCode, contentLength);
        try (var os = exchange.getResponseBody()) {
            if (body.length > 0) {
                os.write(body);
            }
        }
        exchange.close();
    }

    private void sendError(HttpExchange exchange, int statusCode) throws IOException {
        sendResponse(exchange, statusCode, new byte[0]);
    }

    @Override
    public void start() {
        server.start();
        log.info("T1d333KVService started on port {}, storageDir={}", port, storageDir);
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error while closing dao", e);
        }
        log.info("T1d333KVService stopped, storageDir={}", storageDir);
    }
}
