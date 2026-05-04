package company.vk.edu.distrib.compute.nihuaway00.sharding;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class DistributedShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;

    public DistributedShardRouter(String currentNodeEndpoint, ShardingStrategy strategy, HttpClient httpClient) {
        this.currentNodeEndpoint = currentNodeEndpoint;
        this.shardingStrategy = strategy;
        this.httpClient = httpClient;
    }

    @Override
    public String getResponsibleNode(String key) {
        return shardingStrategy.getResponsibleNode(key).getEndpoint();
    }

    @Override
    public void proxyRequest(HttpExchange exchange, String endpoint) throws IOException, InterruptedException {
        HttpRequest proxyRequest = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + exchange.getRequestURI()))
                .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofInputStream(exchange::getRequestBody))
                .build();

        HttpResponse<InputStream> response = httpClient.send(proxyRequest, HttpResponse.BodyHandlers.ofInputStream());

        exchange.sendResponseHeaders(response.statusCode(), 0);
        try (InputStream is = response.body();
             OutputStream os = exchange.getResponseBody()) {
            is.transferTo(os);
        }
    }

    @Override
    public boolean isLocalNode(String endpoint) {
        return currentNodeEndpoint.equals(endpoint);
    }
}
