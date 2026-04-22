package company.vk.edu.distrib.compute.glekoz;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KVServiceGK implements KVService {
    protected static final Logger log = LoggerFactory.getLogger(KVServiceGK.class);
    protected final int port;
    protected static final String ENTITY_PATH = "/v0/entity";
    protected static final String STATUS_PATH = "/v0/status";
    protected static final String ID_PARAM = "id";
    protected static final String CONTENT_TYPE_HEADER = "Content-Type";
    protected static final String OCTET_STREAM = "application/octet-stream";

    protected static final String METHOD_GET = "GET";
    protected static final String METHOD_PUT = "PUT";
    protected static final String METHOD_DELETE = "DELETE";

    protected static final int STATUS_OK = 200;
    protected static final int STATUS_CREATED = 201;
    protected static final int STATUS_ACCEPTED = 202;
    protected static final int STATUS_BAD_REQUEST = 400;
    protected static final int STATUS_NOT_FOUND = 404;
    protected static final int STATUS_METHOD_NOT_ALLOWED = 405;
    
    private static final int SERVER_BACKLOG = 512;

    private final HttpServer server;
    private final FileSystemDao dao;
    // private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public KVServiceGK(int port) {
        this.port = port;
        try {
            this.dao = new FileSystemDao(Path.of(".data", "glekoz", Integer.toString(port)));
            this.server = HttpServer.create(new InetSocketAddress("localhost", this.port), SERVER_BACKLOG);
            initServerContexts();
        } catch (IOException e) {
            log.error("Failed to create HTTP server on port {}", port, e);
            throw new IllegalStateException("Initial server setup failed", e);
        }
    }

    private void initServerContexts() {
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            int status = METHOD_GET.equals(exchange.getRequestMethod()) ? STATUS_OK : STATUS_METHOD_NOT_ALLOWED;
            exchange.sendResponseHeaders(status, -1);
        }
    }

    protected void handleEntity(HttpExchange exchange) {
        try (exchange) {
            String id = extractId(exchange);
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            String method = exchange.getRequestMethod();
            switch (method) {
                case METHOD_GET -> handleGet(exchange, id);
                case METHOD_PUT -> handlePut(exchange, id);
                case METHOD_DELETE -> handleDelete(exchange, id);
                default -> exchange.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, -1);
            }
        } catch (Exception e) {
            log.error("Internal error during request handling", e);
            sendSafeResponse(exchange, STATUS_BAD_REQUEST);
        }
    }

    protected String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.contains("id=")) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                return URLDecoder.decode(param.substring(3), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    protected void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] response = dao.get(id);
            exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);
            exchange.sendResponseHeaders(STATUS_OK, response.length == 0 ? -1 : response.length);
            if (response.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(STATUS_NOT_FOUND, -1);
        }
    }

    protected void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(STATUS_CREATED, -1);
    }

    protected void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(STATUS_ACCEPTED, -1);
    }

    protected void sendSafeResponse(HttpExchange exchange, int code) {
        try {
            exchange.sendResponseHeaders(code, -1);
        } catch (IOException e) {
            log.error("Failed to send error response", e);
        }
    }

    @Override
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.start();
            log.info("Server started on port {}", port);
        }
    }

    @Override
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Stopping server on port {}...", port);
            server.stop(0);
            log.info("Server stopped.");
            dao.close();
            log.info("DAO closed.");
        }
    }
}
