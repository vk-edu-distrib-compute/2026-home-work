package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.usl.sharding.ShardingStrategy;

import java.io.IOException;
import java.util.Objects;

final class EntityHttpHandler implements HttpHandler {
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final Dao<byte[]> dao;
    private final String localEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final ClusterRequestProxy requestProxy;

    EntityHttpHandler(Dao<byte[]> dao) {
        this(dao, null, null, null);
    }

    EntityHttpHandler(
        Dao<byte[]> dao,
        String localEndpoint,
        ShardingStrategy shardingStrategy,
        ClusterRequestProxy requestProxy
    ) {
        this.dao = Objects.requireNonNull(dao);
        this.localEndpoint = localEndpoint;
        this.shardingStrategy = shardingStrategy;
        this.requestProxy = requestProxy;
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void handle(HttpExchange exchange) throws IOException {
        try {
            EntityRequest request = EntityRequestParser.parse(exchange);
            routeRequest(exchange, request);
        } catch (Exception e) {
            EntityErrorResponder.respond(exchange, e);
        } finally {
            exchange.close();
        }
    }

    private void routeRequest(HttpExchange exchange, EntityRequest request) throws IOException, InterruptedException {
        if (shouldProxy(request.key())) {
            handleProxy(exchange, request);
            return;
        }

        handleLocal(exchange, request.method(), request.key(), request.requestBody());
    }

    private void handleLocal(HttpExchange exchange, String method, String key, byte[] requestBody) throws IOException {
        switch (method) {
            case GET_METHOD -> ExchangeResponses.sendBody(exchange, 200, dao.get(key));
            case PUT_METHOD -> handlePut(exchange, key, requestBody);
            case DELETE_METHOD -> handleDelete(exchange, key);
            default -> ExchangeResponses.sendEmpty(exchange, 405);
        }
    }

    private void handleProxy(HttpExchange exchange, EntityRequest request) throws IOException, InterruptedException {
        ClusterRequestProxy.ProxyResponse response = requestProxy.proxy(
            shardingStrategy.resolveOwner(request.key()),
            request.method(),
            exchange.getRequestURI(),
            request.requestBody()
        );
        ExchangeResponses.sendBody(exchange, response.statusCode(), response.body());
    }

    private void handlePut(HttpExchange exchange, String key, byte[] requestBody) throws IOException {
        dao.upsert(key, requestBody);
        ExchangeResponses.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        ExchangeResponses.sendEmpty(exchange, 202);
    }

    private boolean shouldProxy(String key) {
        return shardingStrategy != null && !localEndpoint.equals(shardingStrategy.resolveOwner(key));
    }
}
