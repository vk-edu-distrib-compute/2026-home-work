package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

final class EntityRequestParser {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER = "id";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String QUERY_PARAMETER_SEPARATOR = "&";
    private static final byte[] EMPTY_BODY = new byte[0];

    private EntityRequestParser() {
    }

    static EntityRequest parse(HttpExchange exchange) throws IOException {
        URI requestUri = exchange.getRequestURI();
        if (!ENTITY_PATH.equals(requestUri.getPath())) {
            throw new NoSuchElementException("Unsupported path: " + requestUri.getPath());
        }

        String method = exchange.getRequestMethod();
        if (!isSupportedMethod(method)) {
            throw new MethodNotAllowedException();
        }

        return new EntityRequest(
            extractId(requestUri),
            method,
            readBody(exchange, method)
        );
    }

    private static boolean isSupportedMethod(String method) {
        return GET_METHOD.equals(method) || PUT_METHOD.equals(method) || DELETE_METHOD.equals(method);
    }

    private static byte[] readBody(HttpExchange exchange, String method) throws IOException {
        return switch (method) {
            case PUT_METHOD -> exchange.getRequestBody().readAllBytes();
            case GET_METHOD, DELETE_METHOD -> EMPTY_BODY;
            default -> EMPTY_BODY;
        };
    }

    private static String extractId(URI requestUri) {
        String query = requestUri.getRawQuery();
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing id query parameter");
        }

        String id = null;
        for (String parameter : query.split(QUERY_PARAMETER_SEPARATOR)) {
            id = extractIdValue(parameter, id);
        }

        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Empty id query parameter");
        }

        return id;
    }

    private static String extractIdValue(String parameter, String currentId) {
        int delimiterIndex = parameter.indexOf('=');
        String rawName = delimiterIndex < 0 ? parameter : parameter.substring(0, delimiterIndex);
        if (!ID_PARAMETER.equals(URLDecoder.decode(rawName, StandardCharsets.UTF_8))) {
            return currentId;
        }

        if (currentId != null) {
            throw new IllegalArgumentException("Duplicate id query parameter");
        }

        String rawValue = delimiterIndex < 0 ? "" : parameter.substring(delimiterIndex + 1);
        return URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
    }
}
