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

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class MyReplicatedKVService implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(MyReplicatedKVService.class);

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

    private final int servicePort;
    private final MyReplicaManager replicaManager;
    private final ProxyService proxyService;

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
        this.servicePort = port;
        this.replicaManager = new MyReplicaManager(replicaDaos);
        String selfEndpoint = "http://localhost:" + port;
        this.proxyService = new ProxyService(selfEndpoint, endpoints, shardingStrategy);
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), servicePort),
                    0
            );

            server.createContext("/v0/status", new StatusHandler());
            server.createContext(ENTITY_PATH, new EntityHandler());

            executor = Executors.newFixedThreadPool(DEFAULT_THREADS);
            server.setExecutor(executor);

            server.start();

            if (log.isInfoEnabled()) {
                log.info("KVService started on port {}", servicePort);
            }

        } catch (IOException e) {
            log.error("Failed to start server on port {}", servicePort, e);
            throw new IllegalStateException(e);
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

        try {
            replicaManager.close();
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to close replica manager", e);
            }
        }

        if (log.isInfoEnabled()) {
            log.info("KVService stopped");
        }
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.numberOfReplicas();
    }

    @Override
    public int port() {
        return servicePort;
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
        int ack = Integer.parseInt(ackValue);
        if (ack <= 0) {
            throw new IllegalArgumentException("ack must be positive");
        }
        return ack;
    }

    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }

        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            params.put(
                    URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                    parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : ""
            );
        }
        return params;
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(
                    "GET".equalsIgnoreCase(exchange.getRequestMethod()) ? HTTP_OK : HTTP_METHOD_NOT_ALLOWED,
                    -1
            );
        }
    }

    private final class EntityHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getRawQuery());
            String id = params.get("id");

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                return;
            }

            try {
                int ack = parseAck(params.get("ack"));

                if (ack > numberOfReplicas()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }

                if (proxyService.shouldProxy(exchange, id)) {
                    proxyService.proxy(exchange, id);
                    return;
                }

                handleLocal(exchange, id, ack);

            } catch (Exception e) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private void handleLocal(HttpExchange exchange, String id, int ack) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange, id, ack);
                case "PUT" -> handlePut(exchange, id, ack);
                case "DELETE" -> handleDelete(exchange, id, ack);
                default -> exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            if (replicaManager.enabledReplicas() < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            var res = replicaManager.readLatest(id);

            if (res.confirmations() < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            if (res.latest() == null || res.latest().deleted()) {
                exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
                return;
            }

            byte[] value = res.latest().value();
            exchange.sendResponseHeaders(HTTP_OK, value.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            if (replicaManager.put(id, body) < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }
            exchange.sendResponseHeaders(HTTP_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            if (replicaManager.delete(id) < ack) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }
            exchange.sendResponseHeaders(HTTP_ACCEPTED, -1);
        }
    }

    private static class ProxyService {

        private final HttpClient client;
        private final String self;
        private final List<String> endpoints;
        private final ShardingStrategy strategy;

        ProxyService(String self, List<String> endpoints, ShardingStrategy strategy) {
            this.self = self;
            this.endpoints = endpoints;
            this.strategy = strategy;
            this.client = HttpClient.newHttpClient();
        }

        boolean shouldProxy(HttpExchange exchange, String id) {
            return !endpoints.isEmpty()
                    && !"true".equals(exchange.getRequestHeaders().getFirst(INTERNAL_HEADER))
                    && !self.equals(strategy.resolveOwner(id, endpoints));
        }

        void proxy(HttpExchange exchange, String id) throws IOException {
            String owner = strategy.resolveOwner(id, endpoints);

            URI uri = URI.create(owner + ENTITY_PATH + "?id="
                    + URLEncoder.encode(id, StandardCharsets.UTF_8));

            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header(INTERNAL_HEADER, "true")
                    .method(exchange.getRequestMethod(),
                            "PUT".equals(exchange.getRequestMethod())
                                    ? HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes())
                                    : HttpRequest.BodyPublishers.noBody())
                    .build();

            try {
                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

                exchange.sendResponseHeaders(response.statusCode(), response.body().length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.body());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(HTTP_BAD_GATEWAY, -1);
            }
        }
    }
}
