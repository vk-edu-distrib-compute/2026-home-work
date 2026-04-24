package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

final class CeFelloReplicatedEntityHandler implements HttpHandler {
    private static final String ACK_PARAMETER = "ack";

    private final CeFelloReplicationCoordinator coordinator;
    private final int replicationFactor;

    CeFelloReplicatedEntityHandler(CeFelloReplicationCoordinator coordinator, int replicationFactor) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!CeFelloClusterHttpHelper.ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
            return;
        }

        String method = exchange.getRequestMethod();
        if (!CeFelloClusterHttpHelper.isSupportedEntityMethod(method)) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 405);
            return;
        }

        Map<String, String> queryParameters = CeFelloClusterHttpHelper.parseQuery(
                exchange.getRequestURI().getRawQuery()
        );
        String id = queryParameters.get(CeFelloClusterHttpHelper.ID_PARAMETER);
        if (id == null || id.isEmpty()) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 400);
            return;
        }

        int ack = parseAck(queryParameters.get(ACK_PARAMETER));
        switch (method) {
            case CeFelloClusterHttpHelper.GET_METHOD -> handleGet(exchange, id, ack);
            case CeFelloClusterHttpHelper.PUT_METHOD -> handlePut(exchange, id, ack);
            case CeFelloClusterHttpHelper.DELETE_METHOD -> handleDelete(exchange, id, ack);
            default -> CeFelloClusterHttpHelper.sendEmpty(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        CeFelloReplicationCoordinator.ReadResponse response = coordinator.get(id, ack);
        if (response.found()) {
            CeFelloClusterHttpHelper.sendBody(exchange, 200, response.body());
        } else {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
        }
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        coordinator.upsert(id, exchange.getRequestBody().readAllBytes(), ack);
        CeFelloClusterHttpHelper.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        coordinator.delete(id, ack);
        CeFelloClusterHttpHelper.sendEmpty(exchange, 202);
    }

    private int parseAck(String rawAck) {
        if (rawAck == null || rawAck.isEmpty()) {
            return 1;
        }

        try {
            int ack = Integer.parseInt(rawAck);
            if (ack < 1 || ack > replicationFactor) {
                throw new IllegalArgumentException("Invalid ack value");
            }
            return ack;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid ack value", e);
        }
    }
}
