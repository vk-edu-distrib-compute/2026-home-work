package company.vk.edu.distrib.compute.linempy.scharding;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.linempy.KVServiceImpl;
import company.vk.edu.distrib.compute.linempy.routing.ShardingStrategy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class KVServiceProxyImpl extends KVServiceImpl {
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(5);
    private final HttpClient httpClient = HttpClient.newHttpClient();

    private final String selfEndpoint;
    private final List<String> allEndpoints;
    private final ShardingStrategy shardingStrategy;

    public KVServiceProxyImpl(int port, Dao<byte[]> dao,
                              List<String> allEndpoints,
                              ShardingStrategy shardingStrategy)
            throws IOException {
        super(dao, port);
        this.selfEndpoint = "http://localhost:" + port;
        this.allEndpoints = allEndpoints;
        this.shardingStrategy = shardingStrategy;
    }

    @Override
    protected void entityHandler(HttpExchange exchange) throws IOException {
        String id = detachedId(exchange.getRequestURI().getQuery());
        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        String targetNode = shardingStrategy.route(id, allEndpoints);

        if (selfEndpoint.equals(targetNode)) {
            super.entityHandler(exchange);
        } else {
            proxyRequest(exchange, id, targetNode);
        }
    }

    private void proxyRequest(HttpExchange exchange, String id, String targetNode) throws IOException {
        String targetUrl = targetNode + "/v0/entity?id=" + id;
        String method = exchange.getRequestMethod();

        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder();
            switch (method) {
                case "GET" -> builder = builder.GET();
                case "PUT" -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    builder = builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                }
                case "DELETE" -> builder = builder.DELETE();
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
            }

            HttpRequest request = builder
                    .uri(URI.create(targetUrl))
                    .timeout(PROXY_TIMEOUT)
                    .build();

            HttpResponse<byte[]> proxyResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

            exchange.sendResponseHeaders(proxyResponse.statusCode(),
                    proxyResponse.body() != null ? proxyResponse.body().length : -1);
            if (proxyResponse.body() != null && proxyResponse.body().length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(proxyResponse.body());
                }
            }

        } catch (InterruptedException e) {
            exchange.sendResponseHeaders(500, -1);
        }
    }
}
