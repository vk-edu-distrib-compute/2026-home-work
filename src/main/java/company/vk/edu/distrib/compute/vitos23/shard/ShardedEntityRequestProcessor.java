package company.vk.edu.distrib.compute.vitos23.shard;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.ServerException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.NO_BODY_RESPONSE_LENGTH;

/// [EntityRequestProcessor] implementation for a sharded cluster.
/// If we are the node needed to execute the request, then the request is executed directly.
/// Otherwise, the request is proxied to the target node.
public class ShardedEntityRequestProcessor implements EntityRequestProcessor {

    private final String currentEndpoint;
    private final ShardResolver shardResolver;
    private final EntityRequestProcessor underlyingEntityRequestProcessor;
    private final HttpClient httpClient;

    public ShardedEntityRequestProcessor(
            String currentEndpoint,
            ShardResolver shardResolver,
            EntityRequestProcessor underlyingEntityRequestProcessor,
            HttpClient httpClient
    ) {
        this.currentEndpoint = currentEndpoint;
        this.shardResolver = shardResolver;
        this.underlyingEntityRequestProcessor = underlyingEntityRequestProcessor;
        this.httpClient = httpClient;
    }

    @Override
    public void handleGet(HttpExchange exchange, String id) throws IOException {
        handleRequest(exchange, id, () -> underlyingEntityRequestProcessor.handleGet(exchange, id));
    }

    @Override
    public void handlePut(HttpExchange exchange, String id) throws IOException {
        handleRequest(exchange, id, () -> underlyingEntityRequestProcessor.handlePut(exchange, id));
    }

    @Override
    public void handleDelete(HttpExchange exchange, String id) throws IOException {
        handleRequest(exchange, id, () -> underlyingEntityRequestProcessor.handleDelete(exchange, id));
    }

    private void handleRequest(
            HttpExchange exchange,
            String id,
            IORunnable underlyingAction
    ) throws IOException {
        String targetEndpoint = shardResolver.resolveNode(id);
        if (targetEndpoint.equals(currentEndpoint)) {
            underlyingAction.run();
        } else {
            try {
                proxyHttpExchange(exchange, targetEndpoint);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ServerException(e);
            }
        }
    }

    private void proxyHttpExchange(
            HttpExchange exchange,
            String targetEndpoint
    ) throws IOException, InterruptedException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(getProxiedUri(exchange, targetEndpoint))
                .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        int responseLength = response.body().length > 0 ? response.body().length : NO_BODY_RESPONSE_LENGTH;
        exchange.sendResponseHeaders(response.statusCode(), responseLength);
        if (responseLength > 0) {
            exchange.getResponseBody().write(response.body());
        }
    }

    private static URI getProxiedUri(HttpExchange exchange, String newEndpoint) {
        URI uri = exchange.getRequestURI();
        return URI.create(newEndpoint + uri.getPath() + "?" + uri.getRawQuery());
    }

    @FunctionalInterface
    private interface IORunnable {
        void run() throws IOException;
    }
}
