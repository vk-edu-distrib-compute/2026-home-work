package company.vk.edu.distrib.compute.bobridze5.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

public abstract class AbstractHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange);
                case "POST" -> handlePost(exchange);
                case "PUT" -> handlePut(exchange);
                case "DELETE" -> handleDelete(exchange);
                default -> exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
        }
    }

    protected void handleGet(HttpExchange exchange) throws IOException {
        sendMethodNotAllowed(exchange);
    }

    protected void handlePost(HttpExchange exchange) throws IOException {
        sendMethodNotAllowed(exchange);
    }

    protected void handlePut(HttpExchange exchange) throws IOException {
        sendMethodNotAllowed(exchange);
    }

    protected void handleDelete(HttpExchange exchange) throws IOException {
        sendMethodNotAllowed(exchange);
    }

    protected void sendResponse(HttpExchange exchange, int status, byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            exchange.sendResponseHeaders(status, -1);
        } else {
            exchange.sendResponseHeaders(status, data.length);
            try (var os = exchange.getResponseBody()) {
                os.write(data);
            }
        }
    }

    protected void sendError(HttpExchange exchange, int status) throws IOException {
        String message = switch (status) {
            case HttpURLConnection.HTTP_BAD_REQUEST -> "Bad Request";
            case HttpURLConnection.HTTP_NOT_FOUND -> "Not found";
            case HttpURLConnection.HTTP_INTERNAL_ERROR -> "Internal Server Error";
            case HttpURLConnection.HTTP_BAD_METHOD -> "Method Not Allowed";
            default -> "Error";
        };

        sendResponse(exchange, status, message.getBytes(StandardCharsets.UTF_8));
    }

    private void sendMethodNotAllowed(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
    }

}
