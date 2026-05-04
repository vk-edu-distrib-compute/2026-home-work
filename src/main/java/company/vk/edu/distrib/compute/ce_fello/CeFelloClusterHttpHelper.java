package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class CeFelloClusterHttpHelper {
    static final String STATUS_PATH = "/v0/status";
    static final String ENTITY_PATH = "/v0/entity";
    static final String ID_PARAMETER = "id";
    static final String PROXY_HEADER = "X-CeFello-Proxy";
    static final String PROXY_HEADER_VALUE = "true";
    static final String GET_METHOD = "GET";
    static final String PUT_METHOD = "PUT";
    static final String DELETE_METHOD = "DELETE";

    private CeFelloClusterHttpHelper() {
    }

    static boolean isSupportedEntityMethod(String requestMethod) {
        return GET_METHOD.equals(requestMethod)
                || PUT_METHOD.equals(requestMethod)
                || DELETE_METHOD.equals(requestMethod);
    }

    static boolean isProxied(HttpExchange exchange) {
        String proxyHeader = exchange.getRequestHeaders().getFirst(PROXY_HEADER);
        return PROXY_HEADER_VALUE.equals(proxyHeader);
    }

    static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> parameters = new ConcurrentHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return parameters;
        }

        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";
            parameters.put(key, value);
        }

        return parameters;
    }

    static URI proxyUri(HttpExchange exchange, String ownerEndpoint) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        String suffix = rawQuery == null || rawQuery.isEmpty() ? "" : '?' + rawQuery;
        return URI.create(ownerEndpoint + exchange.getRequestURI().getPath() + suffix);
    }

    static void sendEmpty(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    static void sendBody(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        byte[] responseBody = body == null ? new byte[0] : body;
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(responseBody);
        }
    }

    static int endpointPort(String endpoint) {
        int port = URI.create(endpoint).getPort();
        if (port < 0) {
            throw new IllegalArgumentException("Endpoint must contain an explicit port: " + endpoint);
        }
        return port;
    }
}
