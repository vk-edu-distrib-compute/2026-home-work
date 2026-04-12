package company.vk.edu.distrib.compute.glekoz;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final InetSocketAddress listenAddress;
    private boolean started;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        if (dao == null) {
            throw new IllegalArgumentException("dao must not be null");
        }
        this.dao = dao;
        this.listenAddress = createListenAddress(port);
        this.server = HttpServer.create();
        registerContexts();
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        try {
            server.bind(listenAddress, 0);
            server.start();
            started = true;
            if (log.isDebugEnabled()) {
                log.debug("HTTP server started on {}", server.getAddress());
            }
        } catch (IOException ex) {
            log.error("Failed to start HTTP server on {}", listenAddress, ex);
            throw new UncheckedIOException("Failed to start HTTP server", ex);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service is not started");
        }
        server.stop(0);
        started = false;
        if (log.isDebugEnabled()) {
            log.debug("HTTP server stopped");
        }
    }

    private void registerContexts() {
        server.createContext("/v0/status", wrap(this::handleStatus));
        server.createContext("/v0/entity", wrap(this::handleEntity));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQueryParams(exchange);
        String id = params.get("id");
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }
        String method = exchange.getRequestMethod();
        switch (method) {
            case GET_METHOD: {
                byte[] value = dao.get(id);
                exchange.sendResponseHeaders(200, value.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(value);
                }
                break;
            }
            case PUT_METHOD: {
                byte[] body = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                exchange.sendResponseHeaders(201, -1);
                break;
            }
            case DELETE_METHOD: {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
                break;
            }
            default: {
                exchange.sendResponseHeaders(405, -1);
                break;
            }
        }
    }

    private static Map<String, String> parseQueryParams(HttpExchange exchange) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        ConcurrentMap<String, String> params = new ConcurrentHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";
            params.put(key, value);
        }
        return params;
    }

    private HttpHandler wrap(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Handling {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
                    }
                    handler.handle(exchange);
                } catch (NoSuchElementException ex) {
                    sendError(exchange, 404, ex.getMessage());
                } catch (IllegalArgumentException ex) {
                    sendError(exchange, 400, ex.getMessage());
                } catch (Exception ex) {
                    sendError(exchange, 503, ex.getMessage());
                }
            }
        };
    }

    private static void sendError(HttpExchange exchange, int code, String message) throws IOException {
        String text = message == null ? "" : message;
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static InetSocketAddress createListenAddress(int port) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }
}
