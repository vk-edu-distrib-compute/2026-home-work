package company.vk.edu.distrib.compute.nst1610.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nst1610.sharding.RendezvousStrategy;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);

    private final Dao<byte[]> dao;
    private final String localEndpoint;
    private final RendezvousStrategy sharder;
    private final ClusterProxy clusterProxy;

    public EntityHandler(
        Dao<byte[]> dao,
        String localEndpoint,
        RendezvousStrategy sharder,
        ClusterProxy clusterProxy
    ) {
        this.dao = dao;
        this.localEndpoint = localEndpoint;
        this.sharder = sharder;
        this.clusterProxy = clusterProxy;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                handleRequest(exchange);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException e) {
                log.error("I/O error while handling entity request", e);
                exchange.sendResponseHeaders(500, 0);
            } catch (RuntimeException e) {
                log.error("Unexpected error while handling entity request", e);
                exchange.sendResponseHeaders(500, 0);
            }
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        String id = extractId(exchange.getRequestURI());
        if (id == null) {
            exchange.sendResponseHeaders(400, 0);
            return;
        }
        if (!isSupportedMethod(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, 0);
            return;
        }
        String targetEndpoint = sharder.resolve(id);
        if (!localEndpoint.equals(targetEndpoint)) {
            proxyRequest(exchange, id, targetEndpoint);
            return;
        }
        handleByMethod(exchange, id);
    }

    private void proxyRequest(HttpExchange exchange, String id, String endpoint) throws IOException {
        try {
            ClusterProxy.ProxyResponse response = clusterProxy.forward(
                endpoint,
                id,
                exchange
            );
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            if (response.body().length > 0) {
                exchange.getResponseBody().write(response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Proxy request interrupted", e);
        }
    }

    private void handleByMethod(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        exchange.getResponseBody().write(value);
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        exchange.sendResponseHeaders(201, 0);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, 0);
    }

    private boolean isSupportedMethod(String method) {
        return "GET".equals(method) || "PUT".equals(method) || "DELETE".equals(method);
    }

    private String extractId(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                String value = param.substring(3);
                if (value.isEmpty()) {
                    return null;
                }
                String decodedValue = URLDecoder.decode(value, StandardCharsets.UTF_8);
                return decodedValue.isEmpty() ? null : decodedValue;
            }
        }
        return null;
    }
}
