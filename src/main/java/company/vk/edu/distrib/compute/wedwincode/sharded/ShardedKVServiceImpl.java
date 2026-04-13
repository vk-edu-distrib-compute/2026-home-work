package company.vk.edu.distrib.compute.wedwincode.sharded;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import company.vk.edu.distrib.compute.wedwincode.exceptions.EntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ShardedKVServiceImpl extends KVServiceImpl {
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(5);

    private final Logger log = LoggerFactory.getLogger(ShardedKVServiceImpl.class);
    private final HashStrategy strategy;
    private final String selfEndpoint;

    public ShardedKVServiceImpl(int port, Dao<byte[]> dao, HashStrategy strategy) throws IOException {
        super(port, dao);
        this.server.removeContext("/v0/entity");
        this.server.createContext("/v0/entity", this::handleEntity);
        this.strategy = strategy;
        this.selfEndpoint = "http://localhost:" + port;
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id;
        try {
            id = parseQuery(exchange.getRequestURI().getRawQuery());
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }

        String targetEndpoint = strategy.getEndpoint(id);
        if (!selfEndpoint.equals(targetEndpoint)) {
            URI targetUri = buildEntityUri(targetEndpoint, id);
            proxyRequest(exchange, targetUri);
            return;
        }

        handleEntityMethod(exchange, id);
    }

    private void proxyRequest(HttpExchange exchange, URI uri) {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest.Builder builder = HttpRequest.newBuilder();
            switch (exchange.getRequestMethod()) {
                case GET_METHOD -> builder = builder.GET();
                case PUT_METHOD -> builder = builder.PUT(
                        HttpRequest.BodyPublishers.ofInputStream(exchange::getRequestBody)
                );
                case DELETE_METHOD -> builder = builder.DELETE();
                default -> handleUnsupportedMethod(exchange);
            }

            HttpRequest request = builder
                    .uri(uri)
                    .timeout(PROXY_TIMEOUT)
                    .build();

            HttpResponse<byte[]> proxiedResponse = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

            exchange.sendResponseHeaders(proxiedResponse.statusCode(), proxiedResponse.body().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(proxiedResponse.body());
            }
        } catch (IOException | InterruptedException e) {
            if (log.isErrorEnabled()) {
                log.error("error while proxying: {}", e.getMessage());
            }
        }
    }

    private static URI buildEntityUri(String endpoint, String id) {
        try {
            return new URI(endpoint + "/v0/entity?id=" + id);
        } catch (URISyntaxException e) {
            throw new EntityException("entity URI is invalid", e);
        }
    }
}
