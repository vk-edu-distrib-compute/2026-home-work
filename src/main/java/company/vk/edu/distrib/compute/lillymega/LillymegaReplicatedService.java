package company.vk.edu.distrib.compute.lillymega;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LillymegaReplicatedService implements ReplicatedService {
    private static final String REPLICATION_FACTOR_PROPERTY = "lillymega.replication.factor";
    private static final String REPLICATION_FACTOR_ENV = "LILLYMEGA_REPLICATION_FACTOR";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";
    private static final String REPLICA_STATS_PREFIX = "/stats/replica/";

    private final int port;
    private final int replicationFactor;
    private final HttpServer server;
    private final List<Map<String, VersionedEntry>> replicas = new ArrayList<>();
    private final boolean[] availableReplicas;
    private final AtomicLong[] replicaReadAccess;
    private final AtomicLong[] replicaWriteAccess;
    private final AtomicLong versionGenerator = new AtomicLong();

    public LillymegaReplicatedService(int port) throws IOException {
        this.port = port;
        this.replicationFactor = resolveReplicationFactor();
        this.availableReplicas = new boolean[replicationFactor];
        this.replicaReadAccess = new AtomicLong[replicationFactor];
        this.replicaWriteAccess = new AtomicLong[replicationFactor];
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
        this.server.createContext(REPLICA_STATS_PREFIX, this::handleReplicaStats);

        for (int replicaId = 0; replicaId < replicationFactor; replicaId++) {
            replicas.add(new ConcurrentHashMap<>());
            availableReplicas[replicaId] = true;
            replicaReadAccess[replicaId] = new AtomicLong();
            replicaWriteAccess[replicaId] = new AtomicLong();
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        validateReplicaId(nodeId);
        availableReplicas[nodeId] = false;
    }

    @Override
    public void enableReplica(int nodeId) {
        validateReplicaId(nodeId);
        availableReplicas[nodeId] = true;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            sendEmptyResponse(exchange, 405);
            return;
        }

        sendEmptyResponse(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        RequestParameters parameters = extractParameters(exchange);
        if (parameters == null || parameters.id().isEmpty()) {
            sendEmptyResponse(exchange, 400);
            return;
        }

        if (parameters.ack() < 1 || parameters.ack() > replicationFactor) {
            sendEmptyResponse(exchange, 400);
            return;
        }

        try {
            switch (exchange.getRequestMethod()) {
                case METHOD_GET -> handleGet(exchange, parameters);
                case METHOD_PUT -> handlePut(exchange, parameters);
                case METHOD_DELETE -> handleDelete(exchange, parameters);
                default -> sendEmptyResponse(exchange, 405);
            }
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, 400);
        } catch (NoSuchElementException e) {
            sendEmptyResponse(exchange, 404);
        }
    }

    private void handleReplicaStats(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            sendEmptyResponse(exchange, 405);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(REPLICA_STATS_PREFIX)) {
            sendEmptyResponse(exchange, 404);
            return;
        }

        String suffix = path.substring(REPLICA_STATS_PREFIX.length());
        boolean accessStats = suffix.endsWith("/access");
        String replicaPart = accessStats
                ? suffix.substring(0, suffix.length() - "/access".length())
                : suffix;

        try {
            int replicaId = Integer.parseInt(replicaPart);
            validateReplicaId(replicaId);
            if (accessStats) {
                sendJsonResponse(exchange, replicaAccessStats(replicaId));
            } else {
                sendJsonResponse(exchange, replicaStats(replicaId));
            }
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, 400);
        }
    }

    private void handleGet(HttpExchange exchange, RequestParameters parameters) throws IOException {
        List<Integer> replicaIds = selectReplicas(parameters.id());
        int successfulReads = 0;
        List<VersionedEntry> entries = new ArrayList<>();

        for (int replicaId : replicaIds) {
            if (!availableReplicas[replicaId]) {
                continue;
            }

            replicaReadAccess[replicaId].incrementAndGet();
            successfulReads++;
            entries.add(replicas.get(replicaId).get(parameters.id()));
        }

        if (successfulReads < parameters.ack()) {
            sendEmptyResponse(exchange, 500);
            return;
        }

        VersionedEntry freshest = entries.stream()
                .filter(entry -> entry != null)
                .max(Comparator.comparingLong(VersionedEntry::timestamp))
                .orElseThrow(NoSuchElementException::new);

        if (freshest.deleted()) {
            sendEmptyResponse(exchange, 404);
            return;
        }

        exchange.sendResponseHeaders(200, freshest.value().length);
        exchange.getResponseBody().write(freshest.value());
        exchange.close();
    }

    private void handlePut(HttpExchange exchange, RequestParameters parameters) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        long version = versionGenerator.incrementAndGet();
        VersionedEntry entry = new VersionedEntry(body, version, false);

        int successfulWrites = applyToReplicas(parameters.id(), parameters.ack(), entry);
        if (successfulWrites < parameters.ack()) {
            sendEmptyResponse(exchange, 500);
            return;
        }

        sendEmptyResponse(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, RequestParameters parameters) throws IOException {
        long version = versionGenerator.incrementAndGet();
        VersionedEntry tombstone = new VersionedEntry(new byte[0], version, true);

        int successfulDeletes = applyToReplicas(parameters.id(), parameters.ack(), tombstone);
        if (successfulDeletes < parameters.ack()) {
            sendEmptyResponse(exchange, 500);
            return;
        }

        sendEmptyResponse(exchange, 202);
    }

    private int applyToReplicas(String key, int ack, VersionedEntry entry) {
        int successfulOperations = 0;
        for (int replicaId : selectReplicas(key)) {
            if (!availableReplicas[replicaId]) {
                continue;
            }

            replicas.get(replicaId).put(key, entry);
            replicaWriteAccess[replicaId].incrementAndGet();
            successfulOperations++;
        }

        return successfulOperations;
    }

    private List<Integer> selectReplicas(String key) {
        List<Integer> selectedReplicas = new ArrayList<>(replicationFactor);
        Set<Integer> usedReplicaIds = new HashSet<>();
        long nonce = 0;

        while (selectedReplicas.size() < replicationFactor) {
            int replicaId = Math.floorMod(Long.hashCode(hash(key, nonce)), replicationFactor);
            if (usedReplicaIds.add(replicaId)) {
                selectedReplicas.add(replicaId);
            }
            nonce++;
        }

        return selectedReplicas;
    }

    private long hash(String key, long nonce) {
        String value = key + "#" + nonce;
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private RequestParameters extractParameters(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }

        String id = null;
        Integer ack = null;
        for (String parameter : query.split("&")) {
            String[] parts = parameter.split("=", 2);
            if (parts.length != 2) {
                return null;
            }

            if (ID_PARAMETER.equals(parts[0])) {
                id = parts[1];
            } else if (ACK_PARAMETER.equals(parts[0])) {
                ack = Integer.parseInt(parts[1]);
            }
        }

        return new RequestParameters(id, ack == null ? 1 : ack);
    }

    private void sendEmptyResponse(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
        exchange.close();
    }

    private void sendJsonResponse(HttpExchange exchange, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void validateReplicaId(int nodeId) {
        if (nodeId < 0 || nodeId >= replicationFactor) {
            throw new IllegalArgumentException("Replica id out of range: " + nodeId);
        }
    }

    private int resolveReplicationFactor() {
        String configuredValue = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        if (configuredValue == null || configuredValue.isBlank()) {
            configuredValue = System.getenv(REPLICATION_FACTOR_ENV);
        }
        if (configuredValue == null || configuredValue.isBlank()) {
            return DEFAULT_REPLICATION_FACTOR;
        }

        int parsedValue = Integer.parseInt(configuredValue);
        if (parsedValue < 1) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        return parsedValue;
    }

    private String replicaStats(int replicaId) {
        Map<String, VersionedEntry> replica = replicas.get(replicaId);
        long activeKeys = replica.values().stream()
                .filter(entry -> !entry.deleted())
                .count();
        int payloadBytes = replica.values().stream()
                .filter(entry -> !entry.deleted())
                .mapToInt(entry -> entry.value().length)
                .sum();

        return "{"
                + "\"replicaId\":" + replicaId + ","
                + "\"available\":" + availableReplicas[replicaId] + ","
                + "\"entries\":" + replica.size() + ","
                + "\"activeKeys\":" + activeKeys + ","
                + "\"payloadBytes\":" + payloadBytes
                + "}";
    }

    private String replicaAccessStats(int replicaId) {
        return "{"
                + "\"replicaId\":" + replicaId + ","
                + "\"reads\":" + replicaReadAccess[replicaId].get() + ","
                + "\"writes\":" + replicaWriteAccess[replicaId].get()
                + "}";
    }

    private record RequestParameters(String id, int ack) {
    }

    private record VersionedEntry(byte[] value, long timestamp, boolean deleted) {
    }
}
