package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.artttnik.exception.ServerStartException;
import company.vk.edu.distrib.compute.artttnik.shard.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(MyKVService.class);
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String INTERNAL_HEADER = "X-Internal-Shard-Request";
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_ERROR = 500;
    private static final int HTTP_BAD_GATEWAY = 502;
    private static final int DEFAULT_THREADS = 4;
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String OCTET_STREAM = "application/octet-stream";

    private final int port;
    private final Dao<byte[]> dao;
    private final List<String> endpoints;
    private final String selfEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;

    private ExecutorService executor;
    private HttpServer server;

    public MyKVService(int port, Dao<byte[]> dao) {
        this(port, dao, null, new RendezvousShardingStrategy());
    }

    public MyKVService(int port, Dao<byte[]> dao, List<String> endpoints, ShardingStrategy shardingStrategy) {
        this.port = port;
        this.dao = dao;
        this.selfEndpoint = "http://localhost:" + port;
        this.endpoints = endpoints == null ? List.of() : List.copyOf(new ArrayList<>(endpoints));
        this.shardingStrategy = shardingStrategy;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
                    0
            );

            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler(dao));

            executor = Executors.newFixedThreadPool(DEFAULT_THREADS);
            server.setExecutor(executor);

            server.start();
            log.info("KVService started on port {}", port);
        } catch (IOException e) {
            log.error("Failed to start server on port {}", port, e);
            throw new ServerStartException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }

        if (executor != null) {
            executor.shutdown();
        }

        httpClient.close();

        try {
            dao.close();
        } catch (IOException e) {
            log.debug("Failed to close Dao", e);
        }

        log.info("KVService stopped");
    }

    private static final class StatusHandler implements HttpHandler {
        private static final Logger log = LoggerFactory.getLogger(StatusHandler.class);

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (GET_METHOD.equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(HTTP_OK, -1);
            } else {
                if (log.isErrorEnabled()) {
                    log.error("Unsupported method for /v0/status: {}", exchange.getRequestMethod());
                }
                exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        private static final String ID_PARAM = "id";
        private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);

        private final Dao<byte[]> dao;

        private EntityHandler(Dao<byte[]> dao) {
            this.dao = dao;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var id = extractId(exchange.getRequestURI().getRawQuery());

            if (id == null || id.isEmpty()) {
                log.warn("Bad request: empty id value");
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                return;
            }

            try {
                if (shouldProxy(exchange, id)) {
                    proxy(exchange, id);
                    return;
                }

                dispatch(exchange, id);
            } catch (NoSuchElementException e) {
                log.error("Entity not found: {}", id);
                exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
            } catch (IllegalArgumentException e) {
                if (log.isErrorEnabled()) {
                    log.error("Invalid argument: {}", e.getMessage());
                }
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
            } catch (Exception e) {
                log.error("Internal error processing request", e);
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private boolean shouldProxy(HttpExchange exchange, String id) {
            if (endpoints.isEmpty()) {
                return false;
            }

            if ("true".equalsIgnoreCase(exchange.getRequestHeaders().getFirst(INTERNAL_HEADER))) {
                return false;
            }

            String owner = resolveOwnerEndpoint(id);
            return !selfEndpoint.equals(owner);
        }

        private void proxy(HttpExchange exchange, String id) throws IOException {
            String owner = resolveOwnerEndpoint(id);
            String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
            URI target = URI.create(owner + ENTITY_PATH + "?id=" + encodedId);

            HttpRequest.Builder builder = HttpRequest.newBuilder(target)
                    .timeout(Duration.ofSeconds(2))
                    .header(INTERNAL_HEADER, "true");

            String method = exchange.getRequestMethod();
            if (PUT_METHOD.equals(method)) {
                byte[] body;
                try (InputStream is = exchange.getRequestBody()) {
                    body = is.readAllBytes();
                }
                builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body));
            } else {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            }

            try {
                HttpResponse<byte[]> response =
                        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
                byte[] body = response.body() == null ? new byte[0] : response.body();

                response.headers().firstValue(CONTENT_TYPE_HEADER)
                        .ifPresent(contentType -> exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, contentType));
                if (GET_METHOD.equals(method)
                        && body.length > 0
                        && !exchange.getResponseHeaders().containsKey(CONTENT_TYPE_HEADER)) {
                    exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);
                }

                if (body.length == 0) {
                    exchange.sendResponseHeaders(response.statusCode(), -1);
                    return;
                }

                exchange.sendResponseHeaders(response.statusCode(), body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Proxy request interrupted for id={}", id);
                exchange.sendResponseHeaders(HTTP_BAD_GATEWAY, -1);
            }
        }

        private void dispatch(HttpExchange exchange, String id) throws IOException {
            var method = exchange.getRequestMethod();
            switch (method) {
                case GET_METHOD -> handleGet(exchange, id);
                case PUT_METHOD -> handlePut(exchange, id);
                case DELETE_METHOD -> handleDelete(exchange, id);
                default -> {
                    log.warn("Unsupported method: {}", method);
                    exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
                }
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            var value = dao.get(id);

            exchange.getResponseHeaders().add(CONTENT_TYPE_HEADER, OCTET_STREAM);
            exchange.sendResponseHeaders(HTTP_OK, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            try (InputStream is = exchange.getRequestBody()) {
                var body = is.readAllBytes();
                dao.upsert(id, body);
            }

            exchange.sendResponseHeaders(HTTP_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);

            exchange.sendResponseHeaders(HTTP_ACCEPTED, -1);
        }

        private static String extractId(String query) {
            if (query == null || query.isEmpty()) {
                return null;
            }

            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);

                if (kv.length == 2 && ID_PARAM.equals(kv[0])) {
                    return URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                }
            }

            return null;
        }
    }

    private String resolveOwnerEndpoint(String id) {
        return shardingStrategy.resolveOwner(id, endpoints);
    }
}
