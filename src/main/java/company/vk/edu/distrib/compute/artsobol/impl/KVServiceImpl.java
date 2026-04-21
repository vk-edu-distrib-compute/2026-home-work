package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.artsobol.dao.FileDao;
import company.vk.edu.distrib.compute.artsobol.factory.ShardRouterFactory;
import company.vk.edu.distrib.compute.artsobol.router.ShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private final HttpClient client = HttpClient.newHttpClient();
    private final HttpServer server;
    private final int port;
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final ShardRouter shardRouter;
    private boolean started;
    private boolean stopped;

    public static KVServiceImpl createNode(List<String> endpoints, String endpoint, int port, Path path) {
        return new KVServiceImpl(port, new FileDao(path), endpoints, endpoint);
    }

    public KVServiceImpl(int port, Dao<byte[]> dao) {
        this(port, dao, List.of(endpoint(port)), endpoint(port));
    }

    private KVServiceImpl(int port, Dao<byte[]> dao, List<String> endpoints, String selfEndpoint) {
        try {
            server = HttpServer.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.dao = dao;
        this.port = port;
        this.selfEndpoint = selfEndpoint;
        this.shardRouter = ShardRouterFactory.create(endpoints);
        initServer();
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        if (stopped) {
            throw new IllegalStateException("Service has already been stopped");
        }
        log.info("Server starting on port: {}", port);
        bindServer();
        server.start();
        started = true;
    }

    @Override
    public void stop() {
        if (!started) {
            stopped = true;
            closeDao();
            return;
        }
        log.info("Server stopping");
        server.stop(1);
        started = false;
        stopped = true;
        closeDao();
    }

    private void initServer() {
        apiEndpointStatus();
        apiEndpointEntity();
    }

    private void apiEndpointEntity() {
        server.createContext(
                ENTITY_PATH, new ErrorHttpHandler(http -> {
                    final var method = http.getRequestMethod();
                    final var query = http.getRequestURI().getRawQuery();
                    final var id = parseId(query);

                    String owner = getOwnerEndpoint(id);
                    if (!selfEndpoint.equals(owner)) {
                        proxyTo(owner, http);
                        return;
                    }

                    handleEntityRequest(http, method, id);
                })
        );
    }

    private void proxyTo(String owner, HttpExchange http) throws IOException {
        String method = http.getRequestMethod();
        String query = http.getRequestURI().getRawQuery();
        URI uri = URI.create(owner + ENTITY_PATH + "?" + query);

        HttpRequest.BodyPublisher bodyPublisher;
        if ("PUT".equals(method)) {
            byte[] requestBody = http.getRequestBody().readAllBytes();
            bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(requestBody);
        } else {
            bodyPublisher = HttpRequest.BodyPublishers.noBody();
        }

        HttpRequest request = HttpRequest.newBuilder(uri).method(method, bodyPublisher).build();

        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            writeResponse(http, response.statusCode(), response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void handleEntityRequest(HttpExchange http, String method, String id) throws IOException {
        switch (method) {
            case "GET" -> {
                final var value = dao.get(id);
                writeResponse(http, 200, value);
            }
            case "PUT" -> {
                byte[] value = http.getRequestBody().readAllBytes();
                dao.upsert(id, value);
                writeEmptyResponse(http, 201);
            }
            case "DELETE" -> {
                dao.delete(id);
                writeEmptyResponse(http, 202);
            }
            default -> writeEmptyResponse(http, 405);
        }
    }

    private void apiEndpointStatus() {
        server.createContext(
                STATUS_PATH, http -> {
                    final var method = http.getRequestMethod();
                    if (Objects.equals("GET", method)) {
                        writeEmptyResponse(http, 200);
                    } else {
                        writeEmptyResponse(http, 405);
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

    private String getOwnerEndpoint(String key) {
        return shardRouter.getOwnerEndpoint(key);
    }

    private static String endpoint(int port) {
        return "http://localhost:" + port;
    }

    private static void writeEmptyResponse(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }

    private static void writeResponse(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
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
            writeEmptyResponse(exchange, status);
        }
    }
}
