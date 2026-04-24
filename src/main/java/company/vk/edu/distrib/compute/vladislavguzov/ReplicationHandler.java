package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

class ReplicationHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ReplicationHandler.class);
    private static final int PARAM_PAIR_SIZE = 2;

    private final MyKVCluster cluster;
    private final String[] replicaEndpoints;
    private final boolean[] enabled;
    private final AtomicLong versionCounter;

    private record QueryParams(String id, int ack) {}

    private record ReplicaResponse(int responded, List<VersionedValue> found) {}

    @SuppressWarnings("PMD.ArrayIsStoredDirectly")
    ReplicationHandler(MyKVCluster cluster, String[] replicaEndpoints, boolean... enabled) {
        this.cluster = cluster;
        this.replicaEndpoints = replicaEndpoints.clone();
        this.enabled = enabled;
        this.versionCounter = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
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
                case "ack" -> ack = parseAck(kv[1]);
                default -> { }
            }
        }
        return isValidParams(id, ack) ? new QueryParams(id, ack) : null;
    }

    private static int parseAck(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private boolean isValidParams(String id, int ack) {
        return id != null && !id.isBlank() && ack >= 1 && ack <= replicaEndpoints.length;
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
        for (int i = 0; i < replicaEndpoints.length; i++) {
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
        int successes = writeToReplicas(id, VersionedValue.encode(ts, body));
        exchange.sendResponseHeaders(successes >= ack ? 201 : 500, -1);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        long ts = versionCounter.incrementAndGet();
        int successes = writeToReplicas(id, VersionedValue.encode(ts, null));
        exchange.sendResponseHeaders(successes >= ack ? 202 : 500, -1);
    }

    private int writeToReplicas(String id, byte[] encoded) {
        int successes = 0;
        for (int i = 0; i < replicaEndpoints.length; i++) {
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
}
