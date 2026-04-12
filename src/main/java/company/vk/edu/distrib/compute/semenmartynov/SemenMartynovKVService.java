package company.vk.edu.distrib.compute.semenmartynov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of {@link KVService} using JDK's built-in {@link HttpServer}.
 * Binds to a specific port and handles REST API requests to interact with {@link Dao}.
 */
public class SemenMartynovKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(SemenMartynovKVService.class);

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAM_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final ExecutorService executor;

    /**
     * Constructs a new key-value service on the specified port.
     *
     * @param port the port to bind the HTTP server to
     * @throws IOException if the server cannot be created on the specified port
     */
    public SemenMartynovKVService(int port) throws IOException {
        //this.dao = new InMemoryDao();
        this.dao = new FileSystemDao();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        // Java 21+ Virtual Threads are perfect for lightweight concurrent HTTP handling
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.server.setExecutor(executor);

        this.server.createContext(STATUS_PATH, this::handleStatus);
        this.server.createContext(ENTITY_PATH, this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
        if (log.isInfoEnabled()) {
            log.info("SemenMartynovKVService started on port {}", server.getAddress().getPort());
        }
    }

    @Override
    public void stop() {
        log.info("Stopping SemenMartynovKVService...");
        server.stop(0); // Stop immediately
        executor.shutdownNow();
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Failed to close DAO", e);
        }
        log.info("SemenMartynovKVService stopped.");
    }

    /**
     * Handles the GET /v0/status requests.
     */
    private void handleStatus(HttpExchange exchange) {
        try (exchange) {
            if (!GET_METHOD.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, null); // Method Not Allowed
                return;
            }
            sendResponse(exchange, 200, null);
        } catch (Exception e) {
            log.error("Error in /v0/status handler", e);
        }
    }

    /**
     * Handles requests mapped to /v0/entity (GET, PUT, DELETE).
     * Validates the request path and parameters before processing.
     */
    private void handleEntity(HttpExchange exchange) {
        try (exchange) {
            // Strictly match path (avoid handling /v0/entity/abracadabra here)
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                sendResponse(exchange, 404, null);
                return;
            }

            String id = extractId(exchange.getRequestURI().getQuery());
            if (id == null || id.isEmpty()) {
                sendResponse(exchange, 400, null); // Bad Request: missing or empty ID
                return;
            }

            processEntityRequest(exchange, id);
        } catch (Exception e) {
            log.error("Internal Server Error handling request", e);
            try {
                sendResponse(exchange, 500, null);
            } catch (Exception ignored) {
                // Ignore if response is already sent/closed
            }
        }
    }

    /**
     * Dispatches the validated entity request to the appropriate handler based on HTTP method.
     *
     * @param exchange the HTTP exchange
     * @param id       the entity ID
     * @throws IOException if an I/O error occurs
     */
    private void processEntityRequest(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
        switch (method) {
            case GET_METHOD -> handleGet(exchange, id);
            case PUT_METHOD -> handlePut(exchange, id);
            case DELETE_METHOD -> handleDelete(exchange, id);
            default -> sendResponse(exchange, 405, null); // Method Not Allowed
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            sendResponse(exchange, 200, data);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, 404, null); // Not Found
        } catch (IllegalArgumentException e) {
            sendResponse(exchange, 400, null); // Bad Request
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] value = is.readAllBytes();
            dao.upsert(id, value);
            sendResponse(exchange, 201, null); // Created
        } catch (IllegalArgumentException e) {
            sendResponse(exchange, 400, null);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            sendResponse(exchange, 202, null); // Accepted
        } catch (IllegalArgumentException e) {
            sendResponse(exchange, 400, null);
        }
    }

    /**
     * Helper to extract the 'id' parameter from the query string.
     *
     * @param query the query string from the URI
     * @return the extracted ID, or null if not present
     */
    private String extractId(String query) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith(ID_PARAM_PREFIX)) {
                return param.substring(ID_PARAM_PREFIX.length());
            }
        }
        return null;
    }

    /**
     * Helper to send HTTP responses cleanly.
     *
     * @param exchange   the HTTP exchange
     * @param statusCode the HTTP response code
     * @param body       the byte array body, or null if no body
     * @throws IOException if sending fails
     */
    private void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        if (body == null || body.length == 0) {
            exchange.sendResponseHeaders(statusCode, -1); // -1 means no body
        } else {
            exchange.sendResponseHeaders(statusCode, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
