package company.vk.edu.distrib.compute.lillymega;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LillymegaReplicatedService implements ReplicatedService {
    private final LillymegaReplicaSelector replicaSelector;
    private static final int MIN_ACK = 1;
    private static final int SUCCESS_STATUS = 200;
    private static final int CREATED_STATUS = 201;
    private static final int ACCEPTED_STATUS = 202;
    private static final int BAD_REQUEST_STATUS = 400;
    private static final int NOT_FOUND_STATUS = 404;
    private static final int METHOD_NOT_ALLOWED_STATUS = 405;
    private static final int INTERNAL_ERROR_STATUS = 500;
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String REPLICA_STATS_PREFIX = "/stats/replica/";

    private final int serverPort;
    private final int replicationFactor;
    private final HttpServer server;
    private final List<Map<String, LillymegaVersionedEntry>> replicas = new ArrayList<>();
    private final boolean[] availableReplicas;
    private final AtomicLong[] replicaReadAccess;
    private final AtomicLong[] replicaWriteAccess;
    private final AtomicLong versionGenerator = new AtomicLong();
    private final LillymegaRequestParser requestParser = new LillymegaRequestParser();
    private final LillymegaReplicaStatsFormatter statsFormatter = new LillymegaReplicaStatsFormatter();

    public LillymegaReplicatedService(int port, int replicationFactor) throws IOException {
        this.serverPort = port;
        this.replicationFactor = replicationFactor;
        this.replicaSelector = new LillymegaReplicaSelector(replicationFactor);
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
        return serverPort;
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
            sendEmptyResponse(exchange, METHOD_NOT_ALLOWED_STATUS);
            return;
        }

        sendEmptyResponse(exchange, SUCCESS_STATUS);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        LillymegaRequestParameters parameters = requestParser.parse(exchange.getRequestURI().getQuery());
        if (!hasValidParameters(parameters)) {
            sendEmptyResponse(exchange, BAD_REQUEST_STATUS);
            return;
        }

        try {
            handleEntityByMethod(exchange, parameters);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, BAD_REQUEST_STATUS);
        } catch (NoSuchElementException e) {
            sendEmptyResponse(exchange, NOT_FOUND_STATUS);
        }
    }

    private boolean hasValidParameters(LillymegaRequestParameters parameters) {
        return parameters != null
                && !parameters.id().isEmpty()
                && parameters.ack() >= MIN_ACK
                && parameters.ack() <= replicationFactor;
    }

    private void handleEntityByMethod(HttpExchange exchange, LillymegaRequestParameters parameters) throws IOException {
        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleGet(exchange, parameters);
            case METHOD_PUT -> handlePut(exchange, parameters);
            case METHOD_DELETE -> handleDelete(exchange, parameters);
            default -> sendEmptyResponse(exchange, METHOD_NOT_ALLOWED_STATUS);
        }
    }

    private void handleReplicaStats(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            sendEmptyResponse(exchange, METHOD_NOT_ALLOWED_STATUS);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(REPLICA_STATS_PREFIX)) {
            sendEmptyResponse(exchange, NOT_FOUND_STATUS);
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
                sendJsonResponse(exchange, statsFormatter.replicaAccessStats(
                        replicaId,
                        replicaReadAccess[replicaId],
                        replicaWriteAccess[replicaId]
                ));
            } else {
                sendJsonResponse(exchange, statsFormatter.replicaStats(
                        replicaId,
                        availableReplicas[replicaId],
                        replicas.get(replicaId)
                ));
            }
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, BAD_REQUEST_STATUS);
        }
    }

    private void handleGet(HttpExchange exchange, LillymegaRequestParameters parameters) throws IOException {
        List<Integer> replicaIds = replicaSelector.selectReplicas(parameters.id());
        int successfulReads = 0;
        List<LillymegaVersionedEntry> entries = new ArrayList<>();

        for (int replicaId : replicaIds) {
            if (!availableReplicas[replicaId]) {
                continue;
            }

            replicaReadAccess[replicaId].incrementAndGet();
            successfulReads++;
            entries.add(replicas.get(replicaId).get(parameters.id()));
        }

        if (successfulReads < parameters.ack()) {
            sendEmptyResponse(exchange, INTERNAL_ERROR_STATUS);
            return;
        }

        LillymegaVersionedEntry freshest = entries.stream()
                .filter(entry -> entry != null)
                .max(Comparator.comparingLong(LillymegaVersionedEntry::timestamp))
                .orElseThrow(NoSuchElementException::new);

        if (freshest.deleted()) {
            sendEmptyResponse(exchange, NOT_FOUND_STATUS);
            return;
        }

        exchange.sendResponseHeaders(SUCCESS_STATUS, freshest.value().length);
        exchange.getResponseBody().write(freshest.value());
        exchange.close();
    }

    private void handlePut(HttpExchange exchange, LillymegaRequestParameters parameters) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        long version = versionGenerator.incrementAndGet();
        LillymegaVersionedEntry entry = new LillymegaVersionedEntry(body, version, false);

        int successfulWrites = applyToReplicas(parameters.id(), entry);
        if (successfulWrites < parameters.ack()) {
            sendEmptyResponse(exchange, INTERNAL_ERROR_STATUS);
            return;
        }

        sendEmptyResponse(exchange, CREATED_STATUS);
    }

    private void handleDelete(HttpExchange exchange, LillymegaRequestParameters parameters) throws IOException {
        long version = versionGenerator.incrementAndGet();
        LillymegaVersionedEntry tombstone = new LillymegaVersionedEntry(new byte[0], version, true);

        int successfulDeletes = applyToReplicas(parameters.id(), tombstone);
        if (successfulDeletes < parameters.ack()) {
            sendEmptyResponse(exchange, INTERNAL_ERROR_STATUS);
            return;
        }

        sendEmptyResponse(exchange, ACCEPTED_STATUS);
    }

    private int applyToReplicas(String key, LillymegaVersionedEntry entry) {
        int successfulOperations = 0;
        for (int replicaId : replicaSelector.selectReplicas(key)) {
            if (!availableReplicas[replicaId]) {
                continue;
            }

            replicas.get(replicaId).put(key, entry);
            replicaWriteAccess[replicaId].incrementAndGet();
            successfulOperations++;
        }

        return successfulOperations;
    }

    private void sendEmptyResponse(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
        exchange.close();
    }

    private void sendJsonResponse(HttpExchange exchange, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(SUCCESS_STATUS, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void validateReplicaId(int nodeId) {
        if (nodeId < 0 || nodeId >= replicationFactor) {
            throw new IllegalArgumentException("Replica id out of range: " + nodeId);
        }
    }
}
