package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;

final class ExchangeResponses {
    private ExchangeResponses() {
    }

    static void sendEmpty(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, 0);
        try (OutputStream ignored = exchange.getResponseBody()) {
            // Intentionally empty.
        }
    }

    static void sendBody(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(body);
        }
    }
}
