package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class CeFelloKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(CeFelloKVService.class);

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER = "id";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final InetSocketAddress listenAddress;
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public CeFelloKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = Objects.requireNonNull(dao, "dao");
        this.listenAddress = newListenAddress(port);
        this.server = HttpServer.create();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        createContexts();
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is already started");
        }
        try {
            server.bind(listenAddress, 0);
            server.start();
        } catch (IOException e) {
            started.set(false);
            throw new UncheckedIOException("Failed to start HTTP server", e);
        }
    }

    @Override
    public void stop() {
        if (!started.get() || !stopped.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is not running");
        }
        server.stop(0);
        executor.shutdownNow();
        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Failed to close DAO", e);
        }
    }

    private void createContexts() {
        server.createContext(STATUS_PATH, wrapHandler(this::handleStatus));
        server.createContext(ENTITY_PATH, wrapHandler(this::handleEntity));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
            sendEmpty(exchange, 404);
            return;
        }
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            sendEmpty(exchange, 405);
            return;
        }
        sendEmpty(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            sendEmpty(exchange, 404);
            return;
        }

        String id = parseQuery(exchange).get(ID_PARAMETER);
        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, 400);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> handleGet(exchange, id);
            case PUT_METHOD -> handlePut(exchange, id);
            case DELETE_METHOD -> handleDelete(exchange, id);
            default -> sendEmpty(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(value);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendEmpty(exchange, 202);
    }

    private HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    sendEmpty(exchange, 404);
                } catch (IllegalArgumentException e) {
                    sendEmpty(exchange, 400);
                } catch (Exception e) {
                    log.warn("Failed to handle request", e);
                    sendEmpty(exchange, 503);
                }
            }
        };
    }

    private static Map<String, String> parseQuery(HttpExchange exchange) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        Map<String, String> parameters = new ConcurrentHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return parameters;
        }

        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";
            parameters.put(key, value);
        }
        return parameters;
    }

    private static void sendEmpty(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    @SuppressFBWarnings(
            value = "URLCONNECTION_SSRF_FD",
            justification = "The service binds only to localhost for homework tests"
    )
    private static InetSocketAddress newListenAddress(int port) {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }
}
