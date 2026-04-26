package company.vk.edu.distrib.compute.denchika.proxy;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class KVClusterProxy {
    private final HttpClient httpClient;
    private final Duration timeout;

    public KVClusterProxy(HttpClient httpClient, Duration timeout) {
        this.httpClient = httpClient;
        this.timeout = timeout;
    }

    public void proxy(HttpExchange exchange, String targetUrl, String id) throws IOException {
        try {
            URI uri = URI.create(targetUrl + "/v0/entity?id=" + id);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(timeout);
            String method = exchange.getRequestMethod();
            switch (method) {
                case "GET" -> requestBuilder.GET();
                case "PUT" -> requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(
                        exchange.getRequestBody().readAllBytes()));
                case "DELETE" -> requestBuilder.DELETE();
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
            }
            HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String error = "Proxy interrupted";
            exchange.sendResponseHeaders(500, error.length());
            exchange.getResponseBody().write(error.getBytes());
        } catch (Exception e) {
            String error = "Proxy error: " + e.getMessage();
            exchange.sendResponseHeaders(500, error.length());
            exchange.getResponseBody().write(error.getBytes());
        }
    }
}
