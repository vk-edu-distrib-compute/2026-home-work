package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

final class CeFelloReplicaStatsHandler implements HttpHandler {
    private static final String STATS_PREFIX = "/stats/replica/";

    private final List<CeFelloReplicaNode> replicas;

    CeFelloReplicaStatsHandler(List<CeFelloReplicaNode> replicas) {
        this.replicas = Objects.requireNonNull(replicas, "replicas");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!CeFelloClusterHttpHelper.GET_METHOD.equals(exchange.getRequestMethod())) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 405);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(STATS_PREFIX)) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
            return;
        }

        String suffix = path.substring(STATS_PREFIX.length());
        String[] parts = suffix.split("/");
        if (parts.length == 0 || parts[0].isEmpty()) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
            return;
        }

        int replicaId = parseReplicaId(parts[0]);
        CeFelloReplicaNode replica = replica(replicaId);
        if (parts.length == 1) {
            sendJson(exchange, replicaStatsJson(replica));
            return;
        }
        if (parts.length == 2 && "access".equals(parts[1])) {
            sendJson(exchange, replicaAccessJson(replica));
            return;
        }

        CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
    }

    private CeFelloReplicaNode replica(int replicaId) {
        if (replicaId < 0 || replicaId >= replicas.size()) {
            throw new IllegalArgumentException("Unknown replica id");
        }
        return replicas.get(replicaId);
    }

    private static int parseReplicaId(String rawReplicaId) {
        try {
            return Integer.parseInt(rawReplicaId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replica id", e);
        }
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        CeFelloClusterHttpHelper.sendBody(exchange, 200, json.getBytes(StandardCharsets.UTF_8));
    }

    private static String replicaStatsJson(CeFelloReplicaNode replica) {
        return "{"
                + "\"id\":" + replica.id() + ','
                + "\"enabled\":" + replica.isEnabled() + ','
                + "\"keys\":" + replica.keyCount() + ','
                + "\"bytes\":" + replica.bytesOnDisk()
                + '}';
    }

    private static String replicaAccessJson(CeFelloReplicaNode replica) {
        return "{"
                + "\"id\":" + replica.id() + ','
                + "\"reads\":" + replica.readCount() + ','
                + "\"writes\":" + replica.writeCount() + ','
                + "\"deletes\":" + replica.deleteCount()
                + '}';
    }
}
