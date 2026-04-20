package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

public class ClusterProxyHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(ClusterProxyHandler.class);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private final int localPort;
    private final Dao<byte[]> localDao;
    private final NodesRouter nodesRouter;
    private static final String ID_PREFIX = "id=";

    public ClusterProxyHandler(int localPort, Dao<byte[]> localDao, NodesRouter nodesRouter) {
        this.localPort = localPort;
        this.localDao = localDao;
        this.nodesRouter = nodesRouter;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                String query = exchange.getRequestURI().getQuery();
                if (query == null || !query.startsWith(ID_PREFIX)) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
                String key = query.substring(ID_PREFIX.length());

                ClusterNode target = nodesRouter.get(key);
                if (target == null) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }

                if (target.port() == localPort) {
                    handleLocal(exchange, key);
                } else {
                    forwardRequest(exchange, target, key);
                }
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (Exception e) {
                log.error("Request processing failed", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    private void handleLocal(HttpExchange exchange, String key) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case "GET" -> {
                byte[] value = localDao.get(key);
                exchange.sendResponseHeaders(200, value.length);
                exchange.getResponseBody().write(value);
            }
            case "PUT" -> {
                try (InputStream body = exchange.getRequestBody()) {
                    byte[] value = body.readAllBytes();
                    localDao.upsert(key, value);
                    exchange.sendResponseHeaders(201, 0);
                }
            }
            case "DELETE" -> {
                localDao.delete(key);
                exchange.sendResponseHeaders(202, 0);
            }
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }

    private void forwardRequest(HttpExchange exchange, ClusterNode target, String key)
            throws IOException, InterruptedException {

        String method = exchange.getRequestMethod();
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        String url = target.getUrl() + "/v0/entity?id=" + encodedKey;

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(2));

        switch (method) {
            case "GET" -> {
                builder.GET();
                HttpResponse<byte[]> response = httpClient
                        .send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    byte[] responseBody = response.body();
                    exchange.sendResponseHeaders(200, responseBody.length);
                    exchange.getResponseBody().write(responseBody);
                } else {
                    exchange.sendResponseHeaders(response.statusCode(), -1);
                }
            }
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                HttpResponse<Void> response = httpClient
                        .send(builder.build(),HttpResponse.BodyHandlers.discarding());
                exchange.sendResponseHeaders(response.statusCode(), -1);
            }
            case "DELETE" -> {
                builder.DELETE();
                HttpResponse<Void> response = httpClient
                        .send(builder.build(),HttpResponse.BodyHandlers.discarding());
                exchange.sendResponseHeaders(response.statusCode(), -1);
            }
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }
}
