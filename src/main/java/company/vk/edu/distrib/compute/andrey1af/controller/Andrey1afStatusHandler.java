package company.vk.edu.distrib.compute.andrey1af.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class Andrey1afStatusHandler implements HttpHandler {

    private static final String GET_METHOD = "GET";
    private static final int STATUS_OK = 200;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    @Override
    public void handle(HttpExchange exchange) throws IOException {

        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, -1);
            return;
        }

        String response = "OK";
        exchange.sendResponseHeaders(STATUS_OK, response.length());

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }
}
