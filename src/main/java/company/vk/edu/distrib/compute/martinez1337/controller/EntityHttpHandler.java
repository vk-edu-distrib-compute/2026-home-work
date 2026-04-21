package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;
import company.vk.edu.distrib.compute.martinez1337.util.QueryHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static company.vk.edu.distrib.compute.martinez1337.controller.ResponseStatus.*;

public class EntityHttpHandler extends BaseHttpHandler {
    private static final String ID_PARAM_PREFIX = "id";

    private final Dao<byte[]> dao;

    public EntityHttpHandler(Dao<byte[]> dao, List<String> clusterEndpoints, ShardingStrategy sharding) {
        super(clusterEndpoints, sharding);
        this.dao = dao;
    }

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle GET request", (ex, id) -> {
            byte[] value = dao.get(id);
            ex.sendResponseHeaders(OK.getCode(), value.length);
            ex.getResponseBody().write(value);
        });
    }

    @Override
    protected void handlePut(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle PUT request", (ex, id) -> {
            byte[] value = ex.getRequestBody().readAllBytes();
            dao.upsert(id, value);
            ex.sendResponseHeaders(CREATED.getCode(), 0);
        });
    }

    @Override
    protected void handleDelete(HttpExchange exchange) throws IOException {
        handleIdRequest(exchange, "Handle DELETE request", (ex, id) -> {
            dao.delete(id);
            ex.sendResponseHeaders(ACCEPTED.getCode(), 0);
        });
    }

    private void handleIdRequest(
            HttpExchange exchange,
            String logMessage,
            RequestProcessor processor
    ) throws IOException {
        log.info(logMessage);
        Map<String, List<String>> params = getValidatedParams(exchange);
        String id = params.get(ID_PARAM_PREFIX).getFirst();

        boolean isProxied = exchange.getRequestHeaders().containsKey("X-Proxy");

        Optional<String> targetEndpoint = getTargetProxyEndpoint(id);
        if (targetEndpoint.isPresent() && !isProxied) {
            if (log.isInfoEnabled()) {
                log.info("Proxying request. Target endpoint is {}", targetEndpoint.get());
            }
            proxyRequest(exchange, targetEndpoint.get());
            return;
        }

        processor.process(exchange, id);
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
