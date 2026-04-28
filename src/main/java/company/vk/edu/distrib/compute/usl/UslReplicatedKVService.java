package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public final class UslReplicatedKVService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(UslReplicatedKVService.class);
    private static final int REQUEST_THREADS = 8;
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String REPLICA_STATS_PATH = "/stats/replica";
    private static final String ACCESS_SUFFIX = "access";

    private final HttpServer server;
    private final int port;
    private final ReplicaCoordinator coordinator;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    private ExecutorService requestExecutor;
    private boolean started;
    private boolean stopped;

    public UslReplicatedKVService(int port, int replicaCount, Path storageRoot) throws IOException {
        this.port = port;
        this.server = HttpServer.create();
        this.coordinator = new ReplicaCoordinator(replicaCount, storageRoot);
        initContexts();
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (started) {
                return;
            }
            if (stopped) {
                throw new IllegalStateException("Service has already been stopped");
            }

            requestExecutor = Executors.newFixedThreadPool(REQUEST_THREADS);
            server.setExecutor(requestExecutor);
            server.bind(new InetSocketAddress("localhost", port), 0);
            server.start();
            started = true;
            log.info(
                "Replicated service started on port {} with {} replicas",
                port,
                coordinator.replicaCount()
            );
        } catch (IOException e) {
            shutdownExecutor(requestExecutor);
            requestExecutor = null;
            throw new IllegalStateException("Failed to start replicated service on port " + port, e);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            if (stopped) {
                return;
            }

            if (started) {
                server.stop(0);
            }
            shutdownExecutor(requestExecutor);
            coordinator.close();
            started = false;
            stopped = true;
            log.info("Replicated service stopped on port {}", port);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return coordinator.replicaCount();
    }

    @Override
    public void disableReplica(int nodeId) {
        coordinator.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        coordinator.enableReplica(nodeId);
    }

    private void initContexts() {
        server.createContext(STATUS_PATH, new SafeHandler(this::handleStatus));
        server.createContext(ENTITY_PATH, new SafeHandler(this::handleEntity));
        server.createContext(REPLICA_STATS_PATH, new SafeHandler(this::handleReplicaStats));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            ExchangeResponses.sendEmpty(exchange, 405);
            return;
        }
        ExchangeResponses.sendEmpty(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        ReplicatedEntityRequest request = ReplicatedEntityRequestParser.parse(exchange);
        coordinator.validateAck(request.ack());

        switch (request.method()) {
            case "GET" -> handleGet(exchange, request);
            case "PUT" -> handlePut(exchange, request);
            case "DELETE" -> handleDelete(exchange, request);
            default -> throw new MethodNotAllowedException();
        }
    }

    private void handleGet(HttpExchange exchange, ReplicatedEntityRequest request) throws IOException {
        ReplicaReadResult readResult = coordinator.read(request.key(), request.ack());
        if (readResult.successfulResponses() < request.ack()) {
            ExchangeResponses.sendEmpty(exchange, 500);
            return;
        }

        VersionedValue value = readResult.value();
        if (value == null || value.tombstone()) {
            ExchangeResponses.sendEmpty(exchange, 404);
            return;
        }

        ExchangeResponses.sendBody(exchange, 200, value.body());
    }

    private void handlePut(HttpExchange exchange, ReplicatedEntityRequest request) throws IOException {
        int successfulWrites = coordinator.upsert(request.key(), request.requestBody());
        if (successfulWrites < request.ack()) {
            ExchangeResponses.sendEmpty(exchange, 500);
            return;
        }
        ExchangeResponses.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, ReplicatedEntityRequest request) throws IOException {
        int successfulWrites = coordinator.delete(request.key());
        if (successfulWrites < request.ack()) {
            ExchangeResponses.sendEmpty(exchange, 500);
            return;
        }
        ExchangeResponses.sendEmpty(exchange, 202);
    }

    private void handleReplicaStats(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            ExchangeResponses.sendEmpty(exchange, 405);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String suffix = path.substring(REPLICA_STATS_PATH.length());
        if (suffix.isEmpty() || "/".equals(suffix)) {
            throw new IllegalArgumentException("Missing replica id");
        }

        String[] parts = suffix.substring(1).split("/");
        int replicaId = parseReplicaId(parts[0]);

        if (parts.length == 1) {
            sendJson(exchange, coordinator.replicaStats(replicaId).toJson());
            return;
        }

        if (parts.length == 2 && ACCESS_SUFFIX.equals(parts[1])) {
            sendJson(exchange, coordinator.replicaAccessStats(replicaId).toJson());
            return;
        }

        throw new NoSuchElementException("Unknown stats path");
    }

    private static int parseReplicaId(String rawReplicaId) {
        if (rawReplicaId == null || rawReplicaId.isEmpty()) {
            throw new IllegalArgumentException("Missing replica id");
        }
        return Integer.parseInt(rawReplicaId);
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        ExchangeResponses.sendBody(exchange, 200, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void shutdownExecutor(ExecutorService executorService) {
        if (executorService == null) {
            return;
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface ExchangeDelegate {
        void handle(HttpExchange exchange) throws IOException;
    }

    private record SafeHandler(ExchangeDelegate delegate) implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    ExchangeResponses.sendEmpty(exchange, 400);
                } catch (MethodNotAllowedException e) {
                    ExchangeResponses.sendEmpty(exchange, 405);
                } catch (NoSuchElementException e) {
                    ExchangeResponses.sendEmpty(exchange, 404);
                } catch (IOException e) {
                    ExchangeResponses.sendEmpty(exchange, 500);
                } catch (RuntimeException e) {
                    log.error("Unexpected error while processing request", e);
                    ExchangeResponses.sendEmpty(exchange, 500);
                }
            }
        }
    }
}
