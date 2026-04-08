package company.vk.edu.distrib.compute.andrey1af.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Andrey1afEntityHandler implements HttpHandler {
    private static final String REQUEST_METHOD_GET = "GET";
    private static final String REQUEST_METHOD_PUT = "PUT";
    private static final String REQUEST_METHOD_DELETE = "DELETE";
    private static final String QUERY_PARAMETER_ID_PREFIX = "id=";
    private static final String QUERY_PARAMETER_SEPARATOR = "&";
    private static final int QUERY_PARAMETER_ID_PREFIX_LENGTH = QUERY_PARAMETER_ID_PREFIX.length();
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_BAD_METHOD = 405;

    private final Dao<byte[]> dao;

    public Andrey1afEntityHandler(Dao<byte[]> dao) {
        this.dao = Objects.requireNonNull(dao);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null || id.isBlank()) {
            sendEmpty(exchange, HTTP_BAD_REQUEST);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case REQUEST_METHOD_GET -> handleGet(exchange, id);
            case REQUEST_METHOD_PUT -> handlePut(exchange, id);
            case REQUEST_METHOD_DELETE -> handleDelete(exchange, id);
            default -> sendEmpty(exchange, HTTP_BAD_METHOD);
        }
    }

    private String getId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }

        for (String param : query.split(QUERY_PARAMETER_SEPARATOR)) {
            if (param.startsWith(QUERY_PARAMETER_ID_PREFIX)) {
                return param.substring(QUERY_PARAMETER_ID_PREFIX_LENGTH);
            }
        }
        return null;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try (exchange) {
            try {
                byte[] value = dao.get(id);
                exchange.sendResponseHeaders(HTTP_OK, value.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(value);
                }
            } catch (NoSuchElementException e) {
                sendEmpty(exchange, HTTP_NOT_FOUND);
            }
        }
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
}
