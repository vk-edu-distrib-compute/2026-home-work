package company.vk.edu.distrib.compute.artsobol;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final HttpServer server;
    private final int port;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create();
        this.dao = dao;
        this.port = port;
        initServer();
    }

    @Override
    public void start() {
        log.info("Server starting on port: {}", port);
        bindServer();
        server.start();
    }

    @Override
    public void stop() {
        log.info("Server stopping");
        server.stop(1);
        closeDao();
    }

    private void initServer() {
        apiEndpointStatus();
        apiEndpointEntity();
    }

    private void apiEndpointEntity() {
        server.createContext(
                "/v0/entity", new ErrorHttpHandler(http -> {
                    final var method = http.getRequestMethod();
                    final var query = http.getRequestURI().getQuery();
                    final var id = parseId(query);
                    handleEntityRequest(http, method, id);
                })
        );
    }

    private void handleEntityRequest(HttpExchange http, String method, String id) throws IOException {
        switch (method) {
            case "GET" -> {
                final var value = dao.get(id);
                http.sendResponseHeaders(200, value.length);
                http.getResponseBody().write(value);
            }
            case "PUT" -> {
                byte[] response = http.getRequestBody().readAllBytes();
                dao.upsert(id, response);
                http.sendResponseHeaders(201, -1);
            }
            case "DELETE" -> {
                dao.delete(id);
                http.sendResponseHeaders(202, -1);
            }
            default -> http.sendResponseHeaders(405, -1);
        }
    }

    private void apiEndpointStatus() {
        server.createContext(
                "/v0/status", http -> {
                    final var method = http.getRequestMethod();
                    if (Objects.equals("GET", method)) {
                        http.sendResponseHeaders(200, -1);
                    } else {
                        http.sendResponseHeaders(405, -1);
                    }
                    http.close();
                }
        );
    }

    private static String parseId(String query) {
        String prefix = "id=";
        if (query == null || !query.startsWith(prefix) || query.length() == prefix.length()) {
            throw new IllegalArgumentException("Bad query");
        }
        return query.substring(prefix.length());
    }

    private void bindServer() {
        try {
            server.bind(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind HTTP server to port " + port, e);
        }
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close DAO", e);
        }
    }

    private record ErrorHttpHandler(HttpHandler delegate) implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    sendError(exchange, 400);
                } catch (NoSuchElementException e) {
                    sendError(exchange, 404);
                } catch (IOException e) {
                    sendError(exchange, 500);
                } catch (RuntimeException e) {
                    log.error("Unexpected exception while handling request ", e);
                    sendError(exchange, 500);
                }
            }
        }

        private void sendError(HttpExchange exchange, int status) throws IOException {
            exchange.sendResponseHeaders(status, -1);
        }
    }
}
