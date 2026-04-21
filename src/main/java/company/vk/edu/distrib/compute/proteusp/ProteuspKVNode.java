package company.vk.edu.distrib.compute.proteusp;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class ProteuspKVNode extends ProteuspKVService {

    ShardingAlgorithm sharding;
    List<String> clusterEndpoints;
    String myEndpoint;
    HttpClient httpClient;
    private boolean active = true;

    public ProteuspKVNode(int port, Dao<byte[]> dao) {
        super(port, dao);
    }

    public ProteuspKVNode(int port, Dao<byte[]> dao,
                          List<String> clusterEndpoints,
                          ShardingAlgorithm sharding,
                          String myEndpoint) {
        super(port, dao);
        this.clusterEndpoints = clusterEndpoints;
        this.sharding = sharding;
        this.myEndpoint = myEndpoint;
        this.httpClient = HttpClient.newHttpClient();
    }

    protected void activateNode() {
        active = true;
    }

    protected void deactivateNode() {
        active = false;
    }

    @Override
    protected void handleEntity(HttpExchange exchange) throws IOException {

        String key = getKey(exchange);
        String targetNode = sharding.getNode(key, clusterEndpoints);
        if (targetNode.equals(myEndpoint)) {
            if (active) {
                super.handleEntity(exchange);
            }
        } else {
            proxyToNode(exchange, targetNode, key);
        }
    }

    protected String getKey(HttpExchange exchange) {
        var params = HttpUtils.parseQueryParams(exchange.getRequestURI().getQuery());
        return params.get(PARAM_ID);
    }

    protected void proxyToNode(HttpExchange exchange, String node, String key) throws IOException {

        String method = exchange.getRequestMethod();
        String targetURI = node + "/v0/entity?id=" + key;

        try {
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder().uri(URI.create(targetURI));
            switch (method) {
                case METHOD_PUT -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    reqBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                }
                case METHOD_GET -> reqBuilder.GET();
                case METHOD_DELETE -> reqBuilder.DELETE();
                default -> {
                    return;
                }
            }

            HttpRequest req = reqBuilder.build();
            HttpResponse<byte[]> response = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (IOException e) {
            HttpUtils.sendError(exchange, 503, "Proxy request interrupted");
        } catch (InterruptedException e) {
            HttpUtils.sendError(exchange, 503, "Failed to proxy request: " + e.getMessage());
        }
    }

}
