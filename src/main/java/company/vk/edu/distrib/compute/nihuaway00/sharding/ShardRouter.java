package company.vk.edu.distrib.compute.nihuaway00.sharding;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ShardRouter {
    private final String currentNodeEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;


    public ShardRouter(String currentNodeEndpoint, ShardingStrategy shardingStrategy, HttpClient httpClient) {
        this.currentNodeEndpoint = currentNodeEndpoint;
        this.shardingStrategy = shardingStrategy;
        this.httpClient = httpClient;
    }

    public NodeInfo getNodeEndpoint(String key) {
        return shardingStrategy.getResponsibleNode(key);
    }

    public void proxyRequest(HttpExchange exchange, String targetNodeEndpoint) throws IOException, InterruptedException {
        HttpRequest proxyRequest = HttpRequest.newBuilder()
                .uri(URI.create(targetNodeEndpoint + exchange.getRequestURI()))
                .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofInputStream(exchange::getRequestBody))
                .build();

        HttpResponse<byte[]> response = httpClient.send(proxyRequest, HttpResponse.BodyHandlers.ofByteArray());

        exchange.sendResponseHeaders(response.statusCode(), response.body().length);
        exchange.getResponseBody().write(response.body());
    }

    public boolean isLocalNode(String endpoint) {
        return currentNodeEndpoint.equals(endpoint);
    }
}
