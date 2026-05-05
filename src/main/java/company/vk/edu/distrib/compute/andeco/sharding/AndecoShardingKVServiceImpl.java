package company.vk.edu.distrib.compute.andeco.sharding;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.andeco.KVServiceImpl;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.QueryUtil;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;
import company.vk.edu.distrib.compute.andeco.replica.Controller;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class AndecoShardingKVServiceImpl extends KVServiceImpl {
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(5);
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    private final ShardingStrategy<String> strategy;
    private final Node<String> selfEndpoint;

    public AndecoShardingKVServiceImpl(int port,
                                       ShardingStrategy<String> strategy,
                                       Controller entityController,
                                       Controller statusController) throws IOException {
        super(port, entityController, statusController);
        this.strategy = strategy;
        this.selfEndpoint = new Node<>(null, ServerConfigConstants.LOCALHOST + port);

        server.createContext(ServerConfigConstants.API_PATH
                + ServerConfigConstants.ENTITY_PATH, this::handleEntity);
        server.createContext(ServerConfigConstants.API_PATH
                + ServerConfigConstants.STATUS_PATH, statusController::processRequest);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String entityId;

        try {
            entityId = QueryUtil.extractId(exchange.getRequestURI().getRawQuery());
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return;
        }

        if (entityId == null || entityId.isEmpty()) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return;
        }

        Node<String> targetEndpoint = strategy.get(entityId);
        if (targetEndpoint == null) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_SERVER_ERROR, -1);
            return;
        }

        if (!selfEndpoint.getId().equals(targetEndpoint.getId())) {
            URI targetUri = buildEntityUri(targetEndpoint.getId(), entityId);
            proxyRequest(exchange, targetUri);
            return;
        }

        entityController.processRequest(exchange);
    }

    private void proxyRequest(HttpExchange exchange, URI uri) {
        try (OutputStream os = exchange.getResponseBody()) {
            HttpRequest.Builder builder = HttpRequest.newBuilder();

            switch (Method.valueOf(exchange.getRequestMethod())) {
                case GET -> builder = builder.GET();
                case PUT -> builder = builder.PUT(HttpRequest.BodyPublishers.ofInputStream(exchange::getRequestBody));
                case DELETE -> builder = builder.DELETE();
            }

            HttpRequest request = builder
                    .uri(uri)
                    .timeout(PROXY_TIMEOUT)
                    .build();

            HttpResponse<byte[]> proxiedResponse = CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());

            exchange.sendResponseHeaders(proxiedResponse.statusCode(), proxiedResponse.body().length);
            os.write(proxiedResponse.body());
        } catch (Exception e) {
            throw new IllegalStateException("Unable to proxy request to " + uri, e);
        }
    }

    private static URI buildEntityUri(String endpoint, String id) {
        try {
            return new URI(endpoint + ServerConfigConstants.API_PATH + ServerConfigConstants.ENTITY_PATH + "?id=" + id);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid entity URI for endpoint=" + endpoint + ", id=" + id, e);
        }
    }
}
