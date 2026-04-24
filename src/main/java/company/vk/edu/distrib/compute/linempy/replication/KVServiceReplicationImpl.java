package company.vk.edu.distrib.compute.linempy.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.linempy.HttpCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Реплицированный KV-сервис с поддержкой ack и статистики.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class KVServiceReplicationImpl implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceReplicationImpl.class);

    private final ReplicaManager replicaManager;
    private final ReplicationConfig config;
    private final int serverPort;
    private final HttpServer server;

    public KVServiceReplicationImpl(int serverPort) throws IOException {
        this.serverPort = serverPort;
        this.config = new ReplicationConfig();
        this.replicaManager = new ReplicaManager(config, serverPort);
        this.server = HttpServer.create(new InetSocketAddress(serverPort), 0);

        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
        server.createContext("/stats/replica", this::handleStats);

        log.info("KVServiceReplicationImpl started on port {} with factor={}",
                serverPort, config.getFactor());
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, -1);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String id = extractId(exchange);
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            int ack = extractAck(exchange, config.getDefaultAck());

            if (ack > config.getFactor()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            if (!isEnoughReplicas(id, ack, exchange)) {
                return;
            }

            dispatchRequest(exchange, id, ack);
        } catch (Exception e) {
            log.error("Error handling request", e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private boolean isEnoughReplicas(String id, int ack, HttpExchange exchange) throws IOException {
        int available = replicaManager.getAvailableReplicasCount(id);
        if (available < ack) {
            log.warn("Not enough replicas: need={}, available={}", ack, available);
            exchange.sendResponseHeaders(503, -1);
            return false;
        }
        return true;
    }

    private void dispatchRequest(HttpExchange exchange, String id, int ack) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id, ack);
            case "PUT" -> handlePut(exchange, id, ack);
            case "DELETE" -> handleDelete(exchange, id, ack);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String response;
        int code = HttpCodes.OK;

        try {
            if (path.matches("/stats/replica/\\d+")) {
                String[] parts = path.split("/");
                int replicaId = Integer.parseInt(parts[3]);
                response = handleReplicaStats(replicaId);
            } else if (path.matches("/stats/replica/\\d+/access")) {
                String[] parts = path.split("/");
                int replicaId = Integer.parseInt(parts[3]);
                response = handleReplicaAccess(replicaId);
            } else {
                response = "{\"error\": \"Invalid path. Use /stats/replica/{id} or /stats/replica/{id}/access\"}";
                code = HttpCodes.BAD_REQUEST;
            }
        } catch (Exception e) {
            response = "{\"error\": \"" + e.getMessage() + "\"}";
            code = HttpCodes.SERVER_ERROR;
        }

        byte[] body = response.getBytes();
        exchange.sendResponseHeaders(code, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private String handleReplicaStats(int replicaId) throws IOException {
        long keyCount = replicaManager.getKeyCount(replicaId);
        return String.format("{\"replicaId\": %d, \"keyCount\": %d}", replicaId, keyCount);
    }

    private String handleReplicaAccess(int replicaId) {
        long reads = replicaManager.getReadCount(replicaId);
        long writes = replicaManager.getWriteCount(replicaId);
        return String.format("{\"replicaId\": %d, \"reads\": %d, \"writes\": %d, \"total\": %d}",
                replicaId, reads, writes, reads + writes);
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int success = replicaManager.writeWithAck(id, body, ack);
        exchange.sendResponseHeaders(success >= ack ? HttpCodes.CREATED : HttpCodes.SERVER_ERROR, -1);
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        ReadResult result = replicaManager.readWithAck(id, ack);

        if (!result.success()) {
            log.warn("GET key={}: responded={} < ack={}", id, result.responded(), ack);
            exchange.sendResponseHeaders(HttpCodes.SERVER_ERROR, -1);
            return;
        }

        byte[] value = result.value();
        if (value != null) {
            exchange.sendResponseHeaders(HttpCodes.OK, value.length);
            exchange.getResponseBody().write(value);
        } else {
            exchange.sendResponseHeaders(HttpCodes.NOT_FOUND, -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        int deleted = replicaManager.deleteAllReplicas(id);
        exchange.sendResponseHeaders(deleted >= ack ? HttpCodes.ACCEPTED : HttpCodes.SERVER_ERROR, -1);
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length == 2 && "id".equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }

    private int extractAck(HttpExchange exchange, int defaultValue) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return defaultValue;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                try {
                    return Integer.parseInt(param.substring(4));
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }

    @Override
    public void start() {
        server.start();
        log.info("Server started on port {}", serverPort);
    }

    @Override
    public void stop() {
        replicaManager.close();
        server.stop(0);
        log.info("Server stopped on port {}", serverPort);
    }

    @Override
    public int port() {
        return serverPort;
    }

    @Override
    public int numberOfReplicas() {
        return config.getFactor();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
        log.info("Replica {} disabled", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
        replicaManager.syncReplica(nodeId);
        log.info("Replica {} enabled and synced", nodeId);
    }
}
