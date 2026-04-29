package company.vk.edu.distrib.compute.andrey1af.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.andrey1af.sharding.HashRouter;
import company.vk.edu.distrib.compute.nesterukia.utils.HttpUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Andrey1afEntityHandler implements HttpHandler {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final String REQUEST_METHOD_GET = "GET";
    private static final String REQUEST_METHOD_PUT = "PUT";
    private static final String REQUEST_METHOD_DELETE = "DELETE";
    private static final String INTERNAL_HEADER = "X-Internal-Request";
    private static final String QUERY_PARAMETER_ID_PREFIX = "id=";
    private static final String QUERY_PARAMETER_SEPARATOR = "&";
    private static final int QUERY_PARAMETER_ID_PREFIX_LENGTH = QUERY_PARAMETER_ID_PREFIX.length();
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_BAD_METHOD = 405;
    private static final int HTTP_UNAVAILABLE = 503;

    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final HashRouter shardingStrategy;
    private final String internalRequestToken;

    public Andrey1afEntityHandler(Dao<byte[]> dao, String selfEndpoint, HashRouter shardingStrategy) {
        this(dao, selfEndpoint, shardingStrategy, null);
    }

    public Andrey1afEntityHandler(
            Dao<byte[]> dao, String selfEndpoint, HashRouter shardingStrategy, String internalRequestToken) {
        this.dao = Objects.requireNonNull(dao);
        this.selfEndpoint = selfEndpoint;
        this.shardingStrategy = shardingStrategy;
        this.internalRequestToken = internalRequestToken;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null || id.isBlank()) {
            sendEmpty(exchange, HTTP_BAD_REQUEST);
            return;
        }

        try {
            if (shouldProxy(id, exchange)) {
                proxyRequest(exchange, id);
                return;
            }

            switch (exchange.getRequestMethod()) {
                case REQUEST_METHOD_GET -> handleGet(exchange, id);
                case REQUEST_METHOD_PUT -> handlePut(exchange, id);
                case REQUEST_METHOD_DELETE -> handleDelete(exchange, id);
                default -> sendEmpty(exchange, HTTP_BAD_METHOD);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.close();
            throw new IOException("Interrupted while proxying request", e);
        }
    }

    private String getId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }

        for (String param : query.split(QUERY_PARAMETER_SEPARATOR)) {
            if (param.startsWith(QUERY_PARAMETER_ID_PREFIX)) {
                String encodedId = param.substring(QUERY_PARAMETER_ID_PREFIX_LENGTH);
                return URLDecoder.decode(encodedId, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);
            HttpUtils.sendResponse(exchange, HTTP_OK, value);
        } catch (NoSuchElementException e) {
            sendEmpty(exchange, HTTP_NOT_FOUND);
            return;
        }
        exchange.close();
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        sendEmpty(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendEmpty(exchange, HTTP_ACCEPTED);
    }

    private void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
        exchange.close();
    }

    private boolean shouldProxy(String id, HttpExchange exchange) {
        if (selfEndpoint == null || shardingStrategy == null) {
            return false;
        }
        return !isInternalRequest(exchange)
                && !shardingStrategy.isResponsible(id, selfEndpoint);
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        return internalRequestToken != null
                && internalRequestToken.equals(exchange.getRequestHeaders().getFirst(INTERNAL_HEADER));
    }

    private void proxyRequest(HttpExchange exchange, String id) throws IOException, InterruptedException {
        String targetEndpoint = shardingStrategy.getEndpoint(id);
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        HttpRequest.BodyPublisher bodyPublisher = switch (exchange.getRequestMethod()) {
            case REQUEST_METHOD_PUT -> HttpRequest.BodyPublishers.ofByteArray(requestBody);
            case REQUEST_METHOD_GET, REQUEST_METHOD_DELETE -> HttpRequest.BodyPublishers.noBody();
            default -> {
                sendEmpty(exchange, HTTP_BAD_METHOD);
                yield null;
            }
        };

        if (bodyPublisher == null) {
            return;
        }

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(entityUri(targetEndpoint, id))
                .method(exchange.getRequestMethod(), bodyPublisher);
        if (internalRequestToken != null) {
            requestBuilder.header(INTERNAL_HEADER, internalRequestToken);
        }
        HttpRequest request = requestBuilder.build();

        HttpResponse<byte[]> response;
        try {
            response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            sendEmpty(exchange, HTTP_UNAVAILABLE);
            return;
        }

        byte[] body = response.body();
        if (body.length == 0) {
            sendEmpty(exchange, response.statusCode());
            return;
        }

        exchange.sendResponseHeaders(response.statusCode(), body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        exchange.close();
    }

    private static URI entityUri(String endpoint, String id) {
        return URI.create(endpoint + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8));
    }
}
