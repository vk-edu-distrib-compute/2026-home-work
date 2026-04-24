package company.vk.edu.distrib.compute.marinchanka;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("PMD.GodClass")
public class MarinchankaReplicatedService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(MarinchankaReplicatedService.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String PARAM_ID = "id";
    private static final String PARAM_ACK = "ack";
    private static final String CONTENT_TYPE_VALUE = "application/octet-stream";

    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int INTERNAL_ERROR = 500;
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int SERVICE_UNAVAILABLE = 503;

    private final int servicePort;
    private final int replicationFactor;
    private final List<Dao<byte[]>> replicas;
    private final boolean[] replicaEnabled;
    private HttpServer server;
    private boolean running;

    public MarinchankaReplicatedService(int port, int numberOfReplicas, List<Dao<byte[]>> replicas) {
        this.servicePort = port;
        this.replicationFactor = numberOfReplicas;
        this.replicas = replicas;
        this.replicaEnabled = new boolean[numberOfReplicas];
        Arrays.fill(replicaEnabled, true);
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (nodeId >= 0 && nodeId < replicationFactor) {
            replicaEnabled[nodeId] = false;
            log.info("Replica {} disabled", nodeId);
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        if (nodeId >= 0 && nodeId < replicationFactor) {
            replicaEnabled[nodeId] = true;
            log.info("Replica {} enabled", nodeId);
        }
    }

    @Override
    public void start() {
        if (running) {
            return;
        }

        try {
            server = HttpServer.create(new InetSocketAddress(servicePort), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(null);
            server.start();
            running = true;
            log.info("ReplicatedService started on port {} with {} replicas", servicePort, replicationFactor);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start server on port " + servicePort, e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        if (server != null) {
            server.stop(0);
            server = null; // освобождаем ссылку на сервер
        }
        replicas.forEach(dao -> {
            try {
                dao.close();
            } catch (IOException e) {
                log.error("Error closing DAO", e);
            }
        });
    }

    /**
     * Детерминированно выбирает реплики для ключа.
     */
    private List<Integer> getReplicasForKey(String key) {
        List<Integer> result = new ArrayList<>();
        int hash = Math.abs(key.hashCode());
        for (int i = 0; i < replicationFactor; i++) {
            result.add((hash + i) % replicationFactor);
        }
        return result;
    }

    /**
     * Считает количество доступных реплик для ключа.
     */
    private int availableReplicas(String key) {
        int count = 0;
        for (int replicaId : getReplicasForKey(key)) {
            if (replicaEnabled[replicaId]) {
                count++;
            }
        }
        return count;
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                return;
            }

            if (running) {
                exchange.sendResponseHeaders(STATUS_OK, -1);
            } else {
                exchange.sendResponseHeaders(SERVICE_UNAVAILABLE, -1);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);
            int ack = extractAck(query);

            if (id == null || id.isEmpty()) {
                sendError(exchange, BAD_REQUEST, "Missing id parameter");
                return;
            }

            if (ack > replicationFactor) {
                sendError(exchange, BAD_REQUEST, "ack > numberOfReplicas");
                return;
            }

            handleRequest(exchange, method, id, ack);
        }

        private void handleRequest(HttpExchange exchange, String method, String id, int ack) throws IOException {
            try {
                if (METHOD_GET.equals(method)) {
                    handleGet(exchange, id, ack);
                } else if (METHOD_PUT.equals(method)) {
                    handlePut(exchange, id, ack);
                } else if (METHOD_DELETE.equals(method)) {
                    handleDelete(exchange, id, ack);
                } else {
                    exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, BAD_REQUEST, e.getMessage());
            } catch (NoSuchElementException e) {
                sendError(exchange, NOT_FOUND, e.getMessage());
            } catch (IOException e) {
                log.error("Internal server error", e);
                sendError(exchange, INTERNAL_ERROR, "Internal server error");
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            int available = availableReplicas(id);
            if (available < ack) {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas available");
                return;
            }

            ReadResult result = collectReadResults(id);

            if (result.totalResponses < ack) {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas confirmed");
                return;
            }

            performReadRepair(id, result.maxVersion, result.data, result.tombstone);

            if (result.maxVersion >= 0 && !result.tombstone) {
                sendOkWithData(exchange, result.data);
            } else if (result.maxVersion >= 0) {
                sendError(exchange, NOT_FOUND, "Key not found");
            } else {
                sendError(exchange, NOT_FOUND, "Key not found");
            }
        }

        private ReadResult collectReadResults(String id) {
            Optional<VersionedInMemoryDao.VersionedEntry> best = Optional.empty();
            int foundCount = 0;
            int notFoundCount = 0;

            for (int replicaId : getReplicasForKey(id)) {
                if (!replicaEnabled[replicaId]) {
                    continue;
                }
                ReplicaReadOutcome outcome = processReplicaRead(replicaId, id);
                if (outcome.present) {
                    if (best.isEmpty() || outcome.version > best.get().version) {
                        best = Optional.of(new VersionedInMemoryDao.VersionedEntry(
                                outcome.data, outcome.version, outcome.tombstone));
                    }
                    foundCount++;
                } else {
                    notFoundCount++;
                }
            }

            if (best.isPresent()) {
                VersionedInMemoryDao.VersionedEntry e = best.get();
                return new ReadResult(e.version, e.tombstone ? null : e.data, e.tombstone, foundCount + notFoundCount);
            } else {
                return new ReadResult(-1, null, false, foundCount + notFoundCount);
            }
        }

        private ReplicaReadOutcome processReplicaRead(int replicaId, String id) {
            try {
                VersionedInMemoryDao dao = (VersionedInMemoryDao) replicas.get(replicaId);
                VersionedInMemoryDao.VersionedEntry entry = dao.getEntry(id);
                if (entry != null) {
                    return new ReplicaReadOutcome(true, entry.version, entry.tombstone ? null : entry.data,
                            entry.tombstone);
                }
            } catch (NoSuchElementException e) {
                // ключ не найден на этой реплике
            } catch (IOException e) {
                log.error("Failed to read from replica {}", replicaId, e);
            }
            return new ReplicaReadOutcome(false, -1, null, false);
        }

        private void performReadRepair(String id, long maxVersion, byte[] data, boolean tombstone) {
            if (maxVersion < 0) {
                return;
            }

            for (int replicaId : getReplicasForKey(id)) {
                if (!replicaEnabled[replicaId]) {
                    continue;
                }
                try {
                    VersionedInMemoryDao dao = (VersionedInMemoryDao) replicas.get(replicaId);
                    if (tombstone) {
                        dao.deleteWithVersion(id, maxVersion);
                    } else {
                        dao.upsertWithVersion(id, data, maxVersion);
                    }
                } catch (IOException e) {
                    log.error("Failed to repair replica {}", replicaId, e);
                }
            }
        }

        private void sendOkWithData(HttpExchange exchange, byte[] data) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_VALUE);
            exchange.sendResponseHeaders(STATUS_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            int available = availableReplicas(id);
            if (available < ack) {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas available");
                return;
            }

            byte[] data = exchange.getRequestBody().readAllBytes();
            int confirmed = 0;

            for (int replicaId : getReplicasForKey(id)) {
                if (!replicaEnabled[replicaId]) {
                    continue;
                }

                try {
                    replicas.get(replicaId).upsert(id, data);
                    confirmed++;
                } catch (IOException e) {
                    log.error("Failed to write to replica {}", replicaId, e);
                }
            }

            if (confirmed >= ack) {
                exchange.sendResponseHeaders(STATUS_CREATED, -1);
            } else {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas confirmed");
            }
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            int available = availableReplicas(id);
            if (available < ack) {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas available");
                return;
            }

            int confirmed = 0;

            for (int replicaId : getReplicasForKey(id)) {
                if (!replicaEnabled[replicaId]) {
                    continue;
                }

                try {
                    replicas.get(replicaId).delete(id);
                    confirmed++;
                } catch (IOException e) {
                    log.error("Failed to delete from replica {}", replicaId, e);
                }
            }

            if (confirmed >= ack) {
                exchange.sendResponseHeaders(STATUS_ACCEPTED, -1);
            } else {
                sendError(exchange, INTERNAL_ERROR, "Not enough replicas confirmed");
            }
        }

        private String extractId(String query) {
            return extractParam(query, PARAM_ID);
        }

        private int extractAck(String query) {
            String ackStr = extractParam(query, PARAM_ACK);
            if (ackStr == null) {
                return (replicationFactor / 2) + 1;
            }
            try {
                return Integer.parseInt(ackStr);
            } catch (NumberFormatException e) {
                return 1;
            }
        }

        private String extractParam(String query, String paramName) {
            if (query == null) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && paramName.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void sendError(HttpExchange exchange, int code, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(code, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }

    private static class ReadResult {
        final long maxVersion;
        final byte[] data;
        final boolean tombstone;
        final int totalResponses;

        ReadResult(long maxVersion, byte[] data, boolean tombstone, int totalResponses) {
            this.maxVersion = maxVersion;
            this.data = data != null ? data.clone() : null;
            this.tombstone = tombstone;
            this.totalResponses = totalResponses;
        }
    }

    private static class ReplicaReadOutcome {
        final boolean present;
        final long version;
        final byte[] data;
        final boolean tombstone;

        ReplicaReadOutcome(boolean present, long version, byte[] data, boolean tombstone) {
            this.present = present;
            this.version = version;
            this.data = data != null ? data.clone() : null;
            this.tombstone = tombstone;
        }
    }

    private static class ByteArrayWrapper {
        final byte[] data;

        ByteArrayWrapper(byte[] data) {
            this.data = data.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
