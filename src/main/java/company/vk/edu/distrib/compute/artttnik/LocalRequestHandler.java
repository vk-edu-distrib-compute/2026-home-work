package company.vk.edu.distrib.compute.artttnik;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class LocalRequestHandler {
    static final int DEFAULT_ACK = 1;

    private final MyReplicaManager replicaManager;

    LocalRequestHandler(MyReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    ProxyResult handleLocalRequest(String method, String id, int ack, byte[] body) {
        return switch (method) {
            case "GET" -> handleGetLocal(id, ack);
            case "PUT" -> handlePutLocal(id, ack, body);
            case "DELETE" -> handleDeleteLocal(id, ack);
            default -> new ProxyResult(405, new byte[0]);
        };
    }

    private ProxyResult handleGetLocal(String id, int ack) {
        if (replicaManager.enabledReplicas() < ack) {
            return new ProxyResult(500, new byte[0]);
        }
        var res = replicaManager.readLatest(id);
        if (res.confirmations() < ack) {
            return new ProxyResult(500, new byte[0]);
        }
        if (res.latest() == null || res.latest().deleted()) {
            return new ProxyResult(404, new byte[0]);
        }
        return new ProxyResult(200, res.latest().value());
    }

    private ProxyResult handlePutLocal(String id, int ack, byte[] body) {
        if (replicaManager.put(id, body) < ack) {
            return new ProxyResult(500, new byte[0]);
        }
        return new ProxyResult(201, new byte[0]);
    }

    private ProxyResult handleDeleteLocal(String id, int ack) {
        if (replicaManager.delete(id) < ack) {
            return new ProxyResult(500, new byte[0]);
        }
        return new ProxyResult(202, new byte[0]);
    }

    int parseAck(String ackValue) {
        if (ackValue == null || ackValue.isBlank()) {
            return DEFAULT_ACK;
        }
        int ack = Integer.parseInt(ackValue);
        if (ack <= 0) {
            throw new IllegalArgumentException("ack must be positive");
        }
        return ack;
    }

    static Map<String, String> parseQueryParams(String query) {
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
}
