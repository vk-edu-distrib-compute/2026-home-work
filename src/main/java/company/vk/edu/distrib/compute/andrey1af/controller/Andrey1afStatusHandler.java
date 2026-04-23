package company.vk.edu.distrib.compute.andrey1af.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class Andrey1afStatusHandler implements HttpHandler {

    private static final String GET_METHOD = "GET";

    private static final int STATUS_OK = 200;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private static final String RESPONSE_BODY = "OK";
    private static final String CONTENT_TYPE = "text/plain; charset=UTF-8";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!GET_METHOD.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, -1);
                return;
            }

            byte[] responseBytes = RESPONSE_BODY.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            exchange.sendResponseHeaders(STATUS_OK, responseBytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}
