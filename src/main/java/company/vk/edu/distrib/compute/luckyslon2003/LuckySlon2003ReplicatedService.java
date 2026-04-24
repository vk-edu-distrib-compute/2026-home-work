package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("PMD.GodClass")
public class LuckySlon2003ReplicatedService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(LuckySlon2003ReplicatedService.class);

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";
    private static final String PATH_STATS_REPLICA = "/stats/replica/";
    private static final int BAD_REQUEST = 400;
    private static final int METHOD_NOT_ALLOWED = 405;

    private final int servicePort;
    private final HttpServer server;
    private final List<ReplicaNode> replicas;
    private final ExecutorService replicaExecutor;
    private final AtomicLong versionGenerator = new AtomicLong();

    public LuckySlon2003ReplicatedService(int port, List<Dao<byte[]>> replicaDaos) throws IOException {
        this.servicePort = port;
        this.replicas = createReplicas(replicaDaos);
        this.replicaExecutor = Executors.newFixedThreadPool(Math.max(2, replicaDaos.size()));
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newCachedThreadPool());
        this.server.createContext(PATH_STATUS, this::handleStatus);
        this.server.createContext(PATH_ENTITY, this::handleEntity);
        this.server.createContext(PATH_STATS_REPLICA, this::handleStats);
    }

    @Override
    public void start() {
        server.start();
        if (log.isInfoEnabled()) {
            log.info(
                    "Replicated LuckySlon2003 service started on port {} with {} replicas",
                    servicePort,
                    replicas.size()
            );
        }
    }

    @Override
    public void stop() {
        server.stop(1);
        replicaExecutor.shutdownNow();
        for (ReplicaNode replica : replicas) {
            try {
                replica.dao.close();
            } catch (IOException e) {
                log.error("Failed to close replica {}", replica.id, e);
            }
        }
        log.info("Replicated LuckySlon2003 service stopped");
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        replica(nodeId).enabled.set(false);
    }

    @Override
    public void enableReplica(int nodeId) {
        replica(nodeId).enabled.set(true);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
            sendEmpty(exchange, METHOD_NOT_ALLOWED);
            return;
        }
        sendEmpty(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        EntityRequest entityRequest = parseEntityRequest(exchange);
        if (entityRequest == null) {
            sendEmpty(exchange, BAD_REQUEST);
            return;
        }
        dispatchEntityRequest(exchange, entityRequest);
    }

    private EntityRequest parseEntityRequest(HttpExchange exchange) {
        String id = queryParam(exchange.getRequestURI(), "id");
        if (id == null || id.isEmpty()) {
            return null;
        }

        Integer ack = parseAck(exchange.getRequestURI());
        if (ack == null || ack <= 0 || ack > replicas.size()) {
            return null;
        }
        return new EntityRequest(id, ack);
    }

    private void dispatchEntityRequest(HttpExchange exchange, EntityRequest entityRequest) throws IOException {
        switch (exchange.getRequestMethod().toUpperCase(Locale.ROOT)) {
            case "GET" -> handleGet(exchange, entityRequest.id(), entityRequest.ack());
            case "PUT" -> handlePut(exchange, entityRequest.id(), entityRequest.ack());
            case "DELETE" -> handleDelete(exchange, entityRequest.id(), entityRequest.ack());
            default -> sendEmpty(exchange, METHOD_NOT_ALLOWED);
        }
    }

    private void handleGet(HttpExchange exchange, String key, int ack) throws IOException {
        List<ReplicaReadResult> results = waitForReads(key);
        long successfulReads = results.stream().filter(ReplicaReadResult::successful).count();
        if (successfulReads < ack) {
            sendEmpty(exchange, 500);
            return;
        }

        VersionedEntry freshest = results.stream()
                .filter(ReplicaReadResult::successful)
                .map(ReplicaReadResult::entry)
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(VersionedEntry::version))
                .orElse(null);

        if (freshest == null || freshest.tombstone()) {
            sendEmpty(exchange, 404);
            return;
        }

        byte[] body = freshest.value();
        exchange.sendResponseHeaders(200, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private void handlePut(HttpExchange exchange, String key, int ack) throws IOException {
        byte[] body;
        try (InputStream requestBody = exchange.getRequestBody()) {
            body = requestBody.readAllBytes();
        }

        VersionedEntry entry = new VersionedEntry(nextVersion(), false, body);
        int successCount = waitForWrites(key, entry, false);
        if (successCount < ack) {
            sendEmpty(exchange, 500);
            return;
        }
        sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String key, int ack) throws IOException {
        VersionedEntry tombstone = new VersionedEntry(nextVersion(), true, null);
        int successCount = waitForWrites(key, tombstone, true);
        if (successCount < ack) {
            sendEmpty(exchange, 500);
            return;
        }
        sendEmpty(exchange, 202);
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
            sendEmpty(exchange, METHOD_NOT_ALLOWED);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(PATH_STATS_REPLICA)) {
            sendEmpty(exchange, 404);
            return;
        }

        String suffix = path.substring(PATH_STATS_REPLICA.length());
        if (suffix.isEmpty()) {
            sendEmpty(exchange, 404);
            return;
        }

        boolean accessStats = suffix.endsWith("/access");
        String replicaIdPart = accessStats
                ? suffix.substring(0, suffix.length() - "/access".length())
                : suffix;

        int replicaId;
        try {
            replicaId = Integer.parseInt(replicaIdPart);
        } catch (NumberFormatException e) {
            sendEmpty(exchange, BAD_REQUEST);
            return;
        }

        ReplicaNode replica = replica(replicaId);
        String response = accessStats ? accessStats(replica) : storageStats(replica);
        byte[] body = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private List<ReplicaReadResult> waitForReads(String key) {
        List<CompletableFuture<ReplicaReadResult>> futures = new ArrayList<>(replicas.size());
        for (ReplicaNode replica : replicas) {
            futures.add(CompletableFuture.supplyAsync(() -> readReplica(replica, key), replicaExecutor));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private int waitForWrites(String key, VersionedEntry entry, boolean delete) {
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(replicas.size());
        for (ReplicaNode replica : replicas) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> writeReplica(replica, key, entry, delete),
                    replicaExecutor
            ));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return (int) futures.stream()
                .map(CompletableFuture::join)
                .filter(Boolean.TRUE::equals)
                .count();
    }

    private ReplicaReadResult readReplica(ReplicaNode replica, String key) {
        if (!replica.enabled.get()) {
            return ReplicaReadResult.failure();
        }

        replica.stats.recordRead();
        try {
            byte[] raw = replica.dao.get(key);
            return ReplicaReadResult.success(VersionedEntry.deserialize(raw));
        } catch (NoSuchElementException e) {
            return ReplicaReadResult.success(null);
        } catch (IOException | IllegalArgumentException e) {
            log.warn("Read failed on replica {} for key={}", replica.id, key, e);
            return ReplicaReadResult.failure();
        }
    }

    private boolean writeReplica(ReplicaNode replica, String key, VersionedEntry entry, boolean delete) {
        if (!replica.enabled.get()) {
            return false;
        }

        if (delete) {
            replica.stats.recordDelete();
        } else {
            replica.stats.recordWrite();
        }

        try {
            VersionedEntry previousEntry = readExistingEntry(replica, key);
            replica.dao.upsert(key, entry.serialize());
            replica.stats.adjustCounts(previousEntry, entry);
            return true;
        } catch (IOException | IllegalArgumentException e) {
            log.warn("Write failed on replica {} for key={}", replica.id, key, e);
            return false;
        }
    }

    private VersionedEntry readExistingEntry(ReplicaNode replica, String key) throws IOException {
        try {
            return VersionedEntry.deserialize(replica.dao.get(key));
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private long nextVersion() {
        return versionGenerator.incrementAndGet();
    }

    private ReplicaNode replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Replica not found: " + nodeId);
        }
        return replicas.get(nodeId);
    }

    private static List<ReplicaNode> createReplicas(List<Dao<byte[]>> replicaDaos) {
        List<ReplicaNode> result = new ArrayList<>(replicaDaos.size());
        for (int i = 0; i < replicaDaos.size(); i++) {
            result.add(new ReplicaNode(i, replicaDaos.get(i)));
        }
        return List.copyOf(result);
    }

    private static Integer parseAck(URI uri) {
        String ack = queryParam(uri, "ack");
        if (ack == null) {
            return 1;
        }
        try {
            return Integer.parseInt(ack);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        String prefix = name + "=";
        for (String pair : query.split("&")) {
            if (pair.startsWith(prefix)) {
                return pair.substring(prefix.length());
            }
        }
        return null;
    }

    private static String storageStats(ReplicaNode replica) {
        return "{"
                + "\"replicaId\":" + replica.id + ','
                + "\"enabled\":" + replica.enabled.get() + ','
                + "\"storedKeys\":" + replica.stats.storedKeys() + ','
                + "\"tombstones\":" + replica.stats.tombstones() + ','
                + "\"storedBytes\":" + replica.stats.storedBytes()
                + "}";
    }

    private static String accessStats(ReplicaNode replica) {
        return "{"
                + "\"replicaId\":" + replica.id + ','
                + "\"enabled\":" + replica.enabled.get() + ','
                + "\"reads\":" + replica.stats.readRequests() + ','
                + "\"writes\":" + replica.stats.writeRequests() + ','
                + "\"deletes\":" + replica.stats.deleteRequests()
                + "}";
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static final class ReplicaNode {
        private final int id;
        private final Dao<byte[]> dao;
        private final ReplicaStats stats = new ReplicaStats();
        private final AtomicBoolean enabled = new AtomicBoolean(true);

        private ReplicaNode(int id, Dao<byte[]> dao) {
            this.id = id;
            this.dao = dao;
        }
    }

    private static final class ReplicaReadResult {
        private final boolean successfulRead;
        private final VersionedEntry resolvedEntry;

        private ReplicaReadResult(boolean successful, VersionedEntry entry) {
            this.successfulRead = successful;
            this.resolvedEntry = entry;
        }

        static ReplicaReadResult success(VersionedEntry entry) {
            return new ReplicaReadResult(true, entry);
        }

        static ReplicaReadResult failure() {
            return new ReplicaReadResult(false, null);
        }

        boolean successful() {
            return successfulRead;
        }

        VersionedEntry entry() {
            return resolvedEntry;
        }
    }

    private record EntityRequest(String id, int ack) {
    }
}
