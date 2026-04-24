package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static company.vk.edu.distrib.compute.martinez1337.controller.ResponseStatus.*;

public abstract class BaseHttpHandler implements HttpHandler {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected List<String> clusterEndpoints;
    protected final ShardingStrategy sharding;
    protected int myNodeId;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1))
            .build();

    protected BaseHttpHandler(List<String> clusterEndpoints, ShardingStrategy sharding) {
        this.clusterEndpoints = clusterEndpoints;
        this.sharding = sharding;
    }

    public void setClusterInfo(List<String> clusterEndpoints, int myNodeId) {
        this.clusterEndpoints = clusterEndpoints;
        this.myNodeId = myNodeId;
    }

    @Override
    public final void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET" -> handleGet(exchange);
                    case "PUT" -> handlePut(exchange);
                    case "DELETE" -> handleDelete(exchange);
                    default -> exchange.sendResponseHeaders(405, 0);
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, BAD_REQUEST);
            } catch (NoSuchElementException e) {
                sendError(exchange, NOT_FOUND);
            } catch (UnsupportedOperationException e) {
                sendError(exchange, METHOD_NOT_ALLOWED);
            } catch (Exception e) {
                sendError(exchange, INTERNAL_ERROR);
            }
        }
    }

    protected void handleGet(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected void handlePut(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected void handleDelete(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected final void sendError(HttpExchange exchange, ResponseStatus status) throws IOException {
        if (status.getCode() >= INTERNAL_ERROR.getCode()) {
            if (log.isErrorEnabled()) {
                log.error("{} {}", status.getMessage(), exchange.getRequestURI());
            }
        } else if (log.isDebugEnabled()) {
            log.debug("{} {}", status.getMessage(), exchange.getRequestURI());
        }
        exchange.sendResponseHeaders(status.getCode(), 0);
    }

    protected Optional<String> getTargetProxyEndpoint(String key) {
        if (clusterEndpoints.isEmpty()) {
            return Optional.empty();
        }

        int responsibleNode = sharding.getResponsibleNode(key, clusterEndpoints);
        if (responsibleNode == myNodeId) {
            return Optional.empty();
        }
        return Optional.ofNullable(clusterEndpoints.get(responsibleNode));
    }

    protected void proxyRequest(HttpExchange exchange, String targetEndpoint) throws IOException {
        URI originalUri = exchange.getRequestURI();

        String rawQuery = originalUri.getRawQuery();
        String rawPath = originalUri.getRawPath() != null ? originalUri.getRawPath() : "";

        String targetUri = targetEndpoint
                + rawPath
                + (rawQuery != null ? "?" + rawQuery : "");
        log.debug("Target URI: {}", targetUri);

        String method = exchange.getRequestMethod();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUri))
                .header("X-Proxy", "true")
                .timeout(Duration.ofSeconds(2));

        if ("PUT".equals(method)) {
            byte[] body;
            try (var is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }
            requestBuilder.method(method, body.length > 0
                    ? HttpRequest.BodyPublishers.ofByteArray(body)
                    : HttpRequest.BodyPublishers.noBody());
        } else {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }

        try {
            var req = requestBuilder.build();
            log.debug("Request: {}", req);
            HttpResponse<byte[]> response = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());

            exchange.sendResponseHeaders(response.statusCode(),
                    response.body().length == 0 ? -1 : response.body().length);

            try (var os = exchange.getResponseBody()) {
                if (response.body().length > 0) {
                    os.write(response.body());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Proxying failed", e);
        }
    }
}
