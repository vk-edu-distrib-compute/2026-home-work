package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

final class ReplicatedEntityRequestParser {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final int DEFAULT_ACK = 1;
    private static final byte[] EMPTY_BODY = new byte[0];

    private ReplicatedEntityRequestParser() {
    }

    static ReplicatedEntityRequest parse(HttpExchange exchange) throws IOException {
        URI requestUri = exchange.getRequestURI();
        if (!ENTITY_PATH.equals(requestUri.getPath())) {
            throw new NoSuchElementException("Unsupported path: " + requestUri.getPath());
        }

        String method = exchange.getRequestMethod();
        if (!isSupportedMethod(method)) {
            throw new MethodNotAllowedException();
        }

        Map<String, String> parameters = parseQueryParameters(requestUri.getRawQuery());
        String key = requireId(parameters);
        int ack = parseAck(parameters.get(ACK_PARAMETER));
        byte[] body = PUT_METHOD.equals(method) ? exchange.getRequestBody().readAllBytes() : EMPTY_BODY;

        return new ReplicatedEntityRequest(key, method, ack, body);
    }

    private static boolean isSupportedMethod(String method) {
        return GET_METHOD.equals(method) || PUT_METHOD.equals(method) || DELETE_METHOD.equals(method);
    }

    private static Map<String, String> parseQueryParameters(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            throw new IllegalArgumentException("Missing query string");
        }

        Map<String, String> parameters = new ConcurrentHashMap<>();
        for (String rawParameter : rawQuery.split("&")) {
            int delimiterIndex = rawParameter.indexOf('=');
            String rawName = delimiterIndex < 0 ? rawParameter : rawParameter.substring(0, delimiterIndex);
            String rawValue = delimiterIndex < 0 ? "" : rawParameter.substring(delimiterIndex + 1);
            String name = decode(rawName);
            String value = decode(rawValue);
            if (parameters.putIfAbsent(name, value) != null) {
                throw new IllegalArgumentException("Duplicate query parameter: " + name);
            }
        }
        return parameters;
    }

    private static String requireId(Map<String, String> parameters) {
        String key = parameters.get(ID_PARAMETER);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Missing id");
        }
        return key;
    }

    private static int parseAck(String rawAck) {
        if (rawAck == null) {
            return DEFAULT_ACK;
        }
        if (rawAck.isEmpty()) {
            throw new IllegalArgumentException("Ack must not be empty");
        }
        return Integer.parseInt(rawAck);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
