package company.vk.edu.distrib.compute.vodobryshkin.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vodobryshkin.HttpMethod;
import company.vk.edu.distrib.compute.vodobryshkin.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class EntityHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger("server");

    private static final String ID_KEY = "id";
    private static final int NUMBER_OF_QUERY_STRING_PARTS = 2;

    private final Dao<byte[]> storage;

    public EntityHandler(Dao<byte[]> storage) {
        this.storage = storage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String queryString = exchange.getRequestURI().getQuery();

        log.info("Received a {}-request on /v0/entity?{} endpoint", method, queryString);

        if (!allowed(method)) {
            exchange.sendResponseHeaders(StatusCode.METHOD_NOT_ALLOWED.getCode(), -1);
            return;
        }

        if (malformed(queryString)) {
            exchange.sendResponseHeaders(StatusCode.BAD_REQUEST.getCode(), -1);
            return;
        }

        String[] partsOfQueryString = partsOfQueryString(queryString);

        if (!ID_KEY.equals(partsOfQueryString[0])) {
            exchange.sendResponseHeaders(StatusCode.UNPROCESSABLE_CONTENT.getCode(), -1);
            return;
        }

        String id = partsOfQueryString[1];

        if (method.equals(HttpMethod.GET.name())) {
            handleGet(exchange, id);
        } else if (method.equals(HttpMethod.DELETE.name())) {
            handleDelete(exchange, id);
        } else {
            handlePut(exchange, id);
        }
    }

    private boolean allowed(String method) {
        return method.equals(HttpMethod.GET.name())
                || method.equals(HttpMethod.PUT.name())
                || method.equals(HttpMethod.DELETE.name());
    }

    private boolean malformed(String queryString) {
        if (queryString == null || queryString.isBlank()) {
            return true;
        }

        String[] partsOfQueryString = partsOfQueryString(queryString);

        return partsOfQueryString.length != NUMBER_OF_QUERY_STRING_PARTS
                || partsOfQueryString[1].isEmpty();
    }

    private String[] partsOfQueryString(String queryString) {
        return queryString.split("=", NUMBER_OF_QUERY_STRING_PARTS);
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] result;

        try {
            result = storage.get(id);
        } catch (Exception e) {
            exchange.sendResponseHeaders(StatusCode.NOT_FOUND.getCode(), -1);
            return;
        }

        exchange.sendResponseHeaders(StatusCode.OK.getCode(), result.length);

        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(result);
        }

        log.debug("Successfully handled GET-request on /v0/entity?id={} endpoint with a result: {}", id, result);
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();

        storage.upsert(id, body);
        exchange.sendResponseHeaders(StatusCode.CREATED.getCode(), -1);

        log.debug("Successfully handled PUT-request on /v0/entity?id={}.", id);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        storage.delete(id);
        exchange.sendResponseHeaders(StatusCode.ACCEPTED.getCode(), -1);

        log.debug("Successfully handled DELETE-request on /v0/entity?id={}.", id);
    }
}
