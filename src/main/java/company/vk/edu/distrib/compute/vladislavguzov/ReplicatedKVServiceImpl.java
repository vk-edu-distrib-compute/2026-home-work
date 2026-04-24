package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicatedKVServiceImpl implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private static final int NUM_REPLICAS = 3;
    private static final int PARAM_PAIR_SIZE = 2;

    private final int servicePort;
    private final MyKVCluster cluster;
    private final String[] replicaEndpoints;
    private final boolean[] enabled;
    private final HttpServer coordinatorServer;
    private final AtomicLong versionCounter;

    private record QueryParams(String id, int ack) {}

    private record ReplicaResponse(int responded, List<VersionedValue> found) {}

    public ReplicatedKVServiceImpl(int port) throws IOException {
        this.servicePort = port;
        this.replicaEndpoints = new String[NUM_REPLICAS];
        this.enabled = new boolean[NUM_REPLICAS];
        this.versionCounter = new AtomicLong(System.currentTimeMillis());

        List<Integer> replicaPorts = new ArrayList<>(NUM_REPLICAS);
        for (int i = 0; i < NUM_REPLICAS; i++) {
            int replicaPort = findFreePort();
            replicaPorts.add(replicaPort);
            replicaEndpoints[i] = "localhost:" + replicaPort;
            enabled[i] = true;
        }

        this.cluster = new MyKVCluster(replicaPorts);
        this.coordinatorServer = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        installHandlers();
    }

    @Override
    public void start() {
        cluster.start();
        coordinatorServer.start();
        log.info("Replicated service started on port {}", servicePort);
    }

    @Override
    public void stop() {
        coordinatorServer.stop(0);
        for (int i = 0; i < NUM_REPLICAS; i++) {
            if (enabled[i]) {
                cluster.stop(replicaEndpoints[i]);
                enabled[i] = false;
            }
        }
        log.info("Replicated service stopped");
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return NUM_REPLICAS;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (!enabled[nodeId]) {
            return;
        }
        cluster.stop(replicaEndpoints[nodeId]);
        enabled[nodeId] = false;
        log.info("Replica {} disabled", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        if (enabled[nodeId]) {
            return;
        }
        cluster.start(replicaEndpoints[nodeId]);
        enabled[nodeId] = true;
        log.info("Replica {} enabled", nodeId);
    }

    private void installHandlers() {
        coordinatorServer.createContext("/v0/status", exchange -> {
            try (exchange) {
                int status = "GET".equals(exchange.getRequestMethod()) ? 200 : 405;
                exchange.sendResponseHeaders(status, -1);
            }
        });
        coordinatorServer.createContext("/v0/entity", this::handleEntity);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String query = exchange.getRequestURI().getQuery();
            if (query == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            QueryParams params = parseParams(query);
            if (params == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            routeRequest(exchange, params);
        }
    }

    private QueryParams parseParams(String query) {
        String id = null;
        int ack = 1;
        for (String param : query.split("&")) {
            String[] kv = param.split("=", PARAM_PAIR_SIZE);
            if (kv.length != PARAM_PAIR_SIZE) {
                continue;
            }
            switch (kv[0]) {
                case "id" -> id = kv[1];
                case "ack" -> {
                    try {
                        ack = Integer.parseInt(kv[1]);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
                default -> { }
            }
        }
        if (id == null || id.isBlank() || ack < 1 || ack > NUM_REPLICAS) {
            return null;
        }
        return new QueryParams(id, ack);
    }

    private void routeRequest(HttpExchange exchange, QueryParams params) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, params.id(), params.ack());
            case "PUT" -> handlePut(exchange, params.id(), params.ack());
            case "DELETE" -> handleDelete(exchange, params.id(), params.ack());
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        ReplicaResponse response = collectReplicas(id);
        if (response.responded() < ack) {
            exchange.sendResponseHeaders(500, -1);
            return;
        }
        sendGetResponse(exchange, response.found());
    }

    private ReplicaResponse collectReplicas(String id) {
        List<VersionedValue> found = new ArrayList<>();
        int responded = 0;
        for (int i = 0; i < NUM_REPLICAS; i++) {
            if (!enabled[i]) {
                continue;
            }
            ClusterNode node = cluster.getNode(replicaEndpoints[i]);
            if (node == null) {
                continue;
            }
            try {
                found.add(VersionedValue.decode(node.dao().get(id)));
                responded++;
            } catch (NoSuchElementException e) {
                responded++;
            } catch (IOException e) {
                log.warn("Replica {} GET failed for key {}", i, id, e);
            }
        }
        return new ReplicaResponse(responded, found);
    }

    private void sendGetResponse(HttpExchange exchange, List<VersionedValue> found) throws IOException {
        if (found.isEmpty()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        VersionedValue newest = found.stream()
                .max(Comparator.comparingLong(VersionedValue::timestamp))
                .orElseThrow();
        if (newest.isDeleted()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        byte[] data = newest.data();
        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        long ts = versionCounter.incrementAndGet();
        byte[] encoded = VersionedValue.encode(ts, body);
        int successes = writeToReplicas(id, encoded);
        exchange.sendResponseHeaders(successes >= ack ? 201 : 500, -1);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        long ts = versionCounter.incrementAndGet();
        byte[] tombstone = VersionedValue.encode(ts, null);
        int successes = writeToReplicas(id, tombstone);
        exchange.sendResponseHeaders(successes >= ack ? 202 : 500, -1);
    }

    private int writeToReplicas(String id, byte[] encoded) {
        int successes = 0;
        for (int i = 0; i < NUM_REPLICAS; i++) {
            if (!enabled[i]) {
                continue;
            }
            ClusterNode node = cluster.getNode(replicaEndpoints[i]);
            if (node == null) {
                continue;
            }
            try {
                node.dao().upsert(id, encoded);
                successes++;
            } catch (IOException e) {
                log.warn("Replica {} write failed for key {}", i, id, e);
            }
        }
        return successes;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
