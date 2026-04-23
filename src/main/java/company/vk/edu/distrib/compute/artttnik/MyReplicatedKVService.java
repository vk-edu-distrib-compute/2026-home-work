package company.vk.edu.distrib.compute.artttnik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.artttnik.shard.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyReplicatedKVService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(MyReplicatedKVService.class);
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
    private static final int DEFAULT_ACK = 1;
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String OCTET_STREAM = "application/octet-stream";

    private final int port;
    private final MyReplicaManager replicaManager;
    private final List<String> endpoints;
    private final String selfEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;

    private ExecutorService executor;
    private HttpServer server;

    public MyReplicatedKVService(int port, List<Dao<byte[]>> replicaDaos) {
        this(port, replicaDaos, List.of(), new RendezvousShardingStrategy());
    }

    public MyReplicatedKVService(
            int port,
            List<Dao<byte[]>> replicaDaos,
            List<String> endpoints,
            ShardingStrategy shardingStrategy
    ) {
        this.port = port;
        this.replicaManager = new MyReplicaManager(replicaDaos);
        this.selfEndpoint = "http://localhost:" + port;
        this.endpoints = endpoints == null ? List.of() : List.copyOf(new ArrayList<>(endpoints));
        this.shardingStrategy = shardingStrategy == null ? new RendezvousShardingStrategy() : shardingStrategy;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    public MyReplicatedKVService(int port, Dao<byte[]> dao) {
        this(port, List.of(dao));
    }

    public MyReplicatedKVService(int port, Dao<byte[]> dao, List<String> endpoints, ShardingStrategy shardingStrategy) {
        this(port, List.of(dao), endpoints, shardingStrategy);
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
                    0
            );

            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());

            executor = Executors.newFixedThreadPool(DEFAULT_THREADS);
            server.setExecutor(executor);

            server.start();
            log.info("KVService started on port {}", port);
        } catch (IOException e) {
            log.error("Failed to start server on port {}", port, e);
            throw new IllegalStateException("Failed to start server on port " + port, e);
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
            replicaManager.close();
        } catch (IOException e) {
            log.debug("Failed to close replica manager", e);
        }

        log.info("KVService stopped");
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.numberOfReplicas();
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
    }

    private int parseAck(String ackValue) {
        if (ackValue == null || ackValue.isBlank()) {
            return DEFAULT_ACK;
        }

        try {
            int ack = Integer.parseInt(ackValue);
            if (ack <= 0) {
                throw new IllegalArgumentException("ack must be positive");
            }
            return ack;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid ack value: " + ackValue, e);
        }
    }

    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }

        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            params.put(name, value);
        }

        return params;
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
        String rawQuery = exchange.getRequestURI().getRawQuery();
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        String query = rawQuery == null || rawQuery.isEmpty() ? "id=" + encodedId : rawQuery;
        URI target = URI.create(owner + ENTITY_PATH + "?" + query);

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
            HttpResponse<byte[]> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
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

    private String resolveOwnerEndpoint(String id) {
        return shardingStrategy.resolveOwner(id, endpoints);
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (GET_METHOD.equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(HTTP_OK, -1);
            } else {
                exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        private static final String ID_PARAM = "id";
        private static final String ACK_PARAM = "ack";

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getRawQuery());
            String id = params.get(ID_PARAM);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                return;
            }

            try {
                int ack = parseAck(params.get(ACK_PARAM));

                if (ack > numberOfReplicas()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }

                if (shouldProxy(exchange, id)) {
                    proxy(exchange, id);
                    return;
                }

                dispatch(exchange, id, ack);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
            } catch (Exception e) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private void dispatch(HttpExchange exchange, String id, int ack) throws IOException {
            String method = exchange.getRequestMethod();
            switch (method) {
                case GET_METHOD -> handleGet(exchange, id, ack);
                case PUT_METHOD -> handlePut(exchange, id, ack);
                case DELETE_METHOD -> handleDelete(exchange, id, ack);
                default -> exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            MyReplicaManager.ReplicaReadResult readResult = replicaManager.readLatest(id);
            if (readResult.confirmations() < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            if (readResult.latest() == null || readResult.latest().deleted()) {
                exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
                return;
            }

            exchange.getResponseHeaders().add(CONTENT_TYPE_HEADER, OCTET_STREAM);
            byte[] value = readResult.latest().value();
            exchange.sendResponseHeaders(HTTP_OK, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            byte[] body;
            try (InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }

            int confirmations = replicaManager.put(id, body);
            if (confirmations < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            exchange.sendResponseHeaders(HTTP_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            int confirmations = replicaManager.delete(id);
            if (confirmations < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            exchange.sendResponseHeaders(HTTP_ACCEPTED, -1);
        }
    }
}
