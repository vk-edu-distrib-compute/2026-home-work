package company.vk.edu.distrib.compute.korjick.adapters.input.http.entity;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.Constants;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.KVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class EntityHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);
    private static final String ID_QUERY_PARAM_VALUE = "id";
    private static final String ACK_QUERY_PARAM_VALUE = "ack";

    private final KVCoordinator coordinator;

    public EntityHandler(KVCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        final var method = httpExchange.getRequestMethod();
        final var key = parseIdQueryParam(httpExchange);
        if (key == null) {
            throw new IllegalArgumentException("query param id is required");
        }
        rejectAckQueryParam(httpExchange);
        log.info("Received {} request for entity {}", method, key);
        final var response = process(method, key, httpExchange);
        writeResponse(httpExchange, response);
    }

    private EntityResponse process(String method,
                                   String key,
                                   HttpExchange exchange) throws IOException {
        Entity.Key entityKey = new Entity.Key(key);
        return switch (method) {
            case Constants.HTTP_METHOD_GET -> {
                Entity entity = coordinator.get(entityKey);
                yield new EntityResponse(Constants.HTTP_STATUS_OK, entity.body(), entity.version(), entity.deleted());
            }
            case Constants.HTTP_METHOD_PUT -> {
                byte[] value = exchange.getRequestBody().readAllBytes();
                coordinator.upsert(new Entity(entityKey, value, 0, false));
                yield new EntityResponse(Constants.HTTP_STATUS_CREATED, null, 0, false);
            }
            case Constants.HTTP_METHOD_DELETE -> {
                coordinator.delete(entityKey);
                yield new EntityResponse(Constants.HTTP_STATUS_ACCEPTED, null, 0, false);
            }
            default -> new EntityResponse(Constants.HTTP_STATUS_METHOD_NOT_ALLOWED, null, 0, false);
        };
    }

    private String parseIdQueryParam(HttpExchange httpExchange) {
        final var rawQuery = httpExchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return null;
        }

        return parseQueryParam(rawQuery, ID_QUERY_PARAM_VALUE);
    }

    private void rejectAckQueryParam(HttpExchange httpExchange) {
        final var rawQuery = httpExchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return;
        }
        final var ackValue = parseQueryParam(rawQuery, ACK_QUERY_PARAM_VALUE);
        if (ackValue != null) {
            throw new IllegalArgumentException("query param ack is not supported");
        }
    }

    private String parseQueryParam(String rawQuery, String name) {
        final var params = rawQuery.split(Constants.QUERY_PARAM_SEPARATOR);
        for (var param : params) {
            final var splitParamTuple = param.split(Constants.QUERY_VALUE_SEPARATOR, 2);
            if (splitParamTuple.length == 2 && name.equals(decodeQueryComponent(splitParamTuple[0]))) {
                return decodeQueryComponent(splitParamTuple[1]);
            }
            if (splitParamTuple.length == 1 && name.equals(decodeQueryComponent(splitParamTuple[0]))) {
                return Constants.EMPTY_QUERY_PARAM_VALUE;
            }
        }
        return null;
    }

    private String decodeQueryComponent(String queryComponent) {
        return URLDecoder.decode(queryComponent, StandardCharsets.UTF_8);
    }

    private void writeResponse(HttpExchange exchange, EntityResponse response) throws IOException {
        final byte[] body = response.body();
        if (body == null || body.length == 0) {
            exchange.sendResponseHeaders(response.statusCode(), Constants.EMPTY_BODY_LENGTH);
            return;
        }

        exchange.sendResponseHeaders(response.statusCode(), body.length);
        exchange.getResponseBody().write(body);
    }

    public record EntityResponse(int statusCode, byte[] body, long version, boolean tombstone) {
    }
}
