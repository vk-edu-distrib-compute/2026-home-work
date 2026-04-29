package company.vk.edu.distrib.compute.andrey1af.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class ReplicatedEntityHandler implements HttpHandler {
    private static final int DEFAULT_ACK = 1;
    private static final String QUERY_PARAMETER_ID = "id";
    private static final String QUERY_PARAMETER_ACK = "ack";
    private static final String REQUEST_METHOD_GET = "GET";
    private static final String REQUEST_METHOD_PUT = "PUT";
    private static final String REQUEST_METHOD_DELETE = "DELETE";
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_BAD_METHOD = 405;
    private static final int HTTP_INTERNAL_ERROR = 500;

    private final ReplicatedStorage storage;

    ReplicatedEntityHandler(ReplicatedStorage storage) {
        this.storage = storage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        RequestParameters parameters = parseParameters(exchange);
        if (parameters == null) {
            sendEmpty(exchange, HTTP_BAD_REQUEST);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case REQUEST_METHOD_GET -> handleGet(exchange, parameters);
            case REQUEST_METHOD_PUT -> handlePut(exchange, parameters);
            case REQUEST_METHOD_DELETE -> handleDelete(exchange, parameters);
            default -> sendEmpty(exchange, HTTP_BAD_METHOD);
        }
    }

    private void handleGet(HttpExchange exchange, RequestParameters parameters) throws IOException {
        ReplicatedStorage.ReadResult result = storage.get(parameters.id());
        if (result.responses() < parameters.ack()) {
            sendEmpty(exchange, HTTP_INTERNAL_ERROR);
            return;
        }

        ReplicatedStorage.VersionedRecord record = result.record();
        if (record == null || record.tombstone()) {
            sendEmpty(exchange, HTTP_NOT_FOUND);
            return;
        }

        sendBody(exchange, record.value());
    }

    private void handlePut(HttpExchange exchange, RequestParameters parameters) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        int successfulWrites = storage.upsert(parameters.id(), value);

        if (successfulWrites < parameters.ack()) {
            sendEmpty(exchange, HTTP_INTERNAL_ERROR);
            return;
        }

        sendEmpty(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, RequestParameters parameters) throws IOException {
        int successfulWrites = storage.delete(parameters.id());

        if (successfulWrites < parameters.ack()) {
            sendEmpty(exchange, HTTP_INTERNAL_ERROR);
            return;
        }

        sendEmpty(exchange, HTTP_ACCEPTED);
    }

    private RequestParameters parseParameters(HttpExchange exchange) {
        Map<String, String> queryParameters = parseQuery(exchange.getRequestURI().getRawQuery());
        String id = queryParameters.get(QUERY_PARAMETER_ID);
        if (id == null || id.isBlank()) {
            return null;
        }

        int ack = DEFAULT_ACK;
        String ackParameter = queryParameters.get(QUERY_PARAMETER_ACK);
        if (ackParameter != null) {
            try {
                ack = Integer.parseInt(ackParameter);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        if (ack < 1 || ack > storage.numberOfReplicas()) {
            return null;
        }

        return new RequestParameters(id, ack);
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> result = new ConcurrentHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return result;
        }

        try {
            for (String parameter : rawQuery.split("&")) {
                putQueryParameter(result, parameter);
            }
        } catch (IllegalArgumentException e) {
            result.clear();
        }

        return result;
    }

    private static void putQueryParameter(Map<String, String> result, String parameter) {
        int delimiter = parameter.indexOf('=');
        if (delimiter < 0) {
            result.put(decode(parameter), "");
            return;
        }

        String key = decode(parameter.substring(0, delimiter));
        String value = decode(parameter.substring(delimiter + 1));
        result.put(key, value);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
        exchange.close();
    }

    private static void sendBody(HttpExchange exchange, byte[] body) throws IOException {
        exchange.sendResponseHeaders(HTTP_OK, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
        exchange.close();
    }

    private record RequestParameters(String id, int ack) {
    }
}
