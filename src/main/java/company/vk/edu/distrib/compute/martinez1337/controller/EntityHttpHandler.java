package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;
import company.vk.edu.distrib.compute.martinez1337.replication.ReplicationManager;
import company.vk.edu.distrib.compute.martinez1337.util.QueryHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static company.vk.edu.distrib.compute.martinez1337.controller.ResponseStatus.*;

public class EntityHttpHandler extends BaseHttpHandler {
    private static final String ID_PARAM_PREFIX = "id";
    private static final String ACK_PARAM_PREFIX = "ack";

    private final ReplicationManager replicationManager;

    public EntityHttpHandler(
            List<String> clusterEndpoints,
            ShardingStrategy sharding,
            ReplicationManager replicationManager
    ) {
        super(clusterEndpoints, sharding);
        this.replicationManager = replicationManager;
    }

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle GET request", (ex, id, ack) -> {
            List<Integer> replicasForKey = replicationManager.selectReplicas(id);
            var readResult = replicationManager.get(id, ack, replicasForKey);
            if (readResult.statusCode() == OK.getCode()) {
                byte[] value = readResult.body();
                ex.sendResponseHeaders(OK.getCode(), value.length);
                ex.getResponseBody().write(value);
                return;
            }
            ex.sendResponseHeaders(readResult.statusCode(), -1);
        });
    }

    @Override
    protected void handlePut(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle PUT request", (ex, id, ack) -> {
            byte[] value = ex.getRequestBody().readAllBytes();
            List<Integer> replicasForKey = replicationManager.selectReplicas(id);
            int status = replicationManager.upsert(id, value, ack, replicasForKey);
            ex.sendResponseHeaders(status, -1);
        });
    }

    @Override
    protected void handleDelete(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle DELETE request", (ex, id, ack) -> {
            List<Integer> replicasForKey = replicationManager.selectReplicas(id);
            int status = replicationManager.delete(id, ack, replicasForKey);
            ex.sendResponseHeaders(status, -1);
        });
    }

    private void handleIdRequest(
            HttpExchange exchange,
            String logMessage,
            RequestProcessor processor
    ) throws IOException {
        log.info(logMessage);
        Map<String, List<String>> params = getValidatedParams(exchange);
        String id = getIdQueryParam(params);
        Integer ack = getAckQueryParam(params);

        boolean isProxied = exchange.getRequestHeaders().containsKey("X-Proxy");

        Optional<String> targetEndpoint = getTargetProxyEndpoint(id);
        if (targetEndpoint.isPresent() && !isProxied) {
            if (log.isInfoEnabled()) {
                log.info("Proxying request. Target endpoint is {}", targetEndpoint.get());
            }
            proxyRequest(exchange, targetEndpoint.get());
            return;
        }

        processor.process(exchange, id, ack);
    }

    private String getIdQueryParam(Map<String, List<String>> params) {
        String id = params.get(ID_PARAM_PREFIX).getFirst();
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Id parameter must not be empty");
        }
        return id;
    }

    private Integer getAckQueryParam(Map<String, List<String>> params) {
        List<String> values = params.get(ACK_PARAM_PREFIX);
        if (values == null || values.isEmpty()) {
            return 1;
        }
        if (values.size() > 1) {
            throw new IllegalArgumentException("Ack parameter must be provided once");
        }
        String ackStr = values.getFirst();
        int ack;
        try {
            ack = Integer.parseInt(ackStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ack parameter must be an integer", e);
        }
        if (ack <= 0 || ack > replicationManager.replicasCount()) {
            throw new IllegalArgumentException("Ack value is out of allowed range");
        }
        return ack;
    }

    private static void validateParams(Map<String, List<String>> params) {
        if (params.isEmpty() || !params.containsKey(ID_PARAM_PREFIX)) {
            throw new IllegalArgumentException("bad query");
        }
    }

    private static Map<String, List<String>> getValidatedParams(HttpExchange exchange) {
        Map<String, List<String>> params = QueryHelper.parseParams(exchange.getRequestURI().getRawQuery());
        validateParams(params);
        return params;
    }
}
