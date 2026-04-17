package company.vk.edu.distrib.compute.v11qfour.proxy;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.v11qfour.cluster.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class V11qfourProxyClient {
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public void proxy(HttpExchange exchange, V11qfourNode targetNode) throws IOException {
        String method = exchange.getRequestMethod();
        String uri = targetNode.url() + exchange.getRequestURI().getPath() + "?" + exchange.getRequestURI().getQuery();
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(uri));

        if ("PUT".equals(method)) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        } else if ("GET".equals(method)) {
            builder.GET();
        } else if ("DELETE".equals(method)) {
            builder.DELETE();
        }

        try {
            HttpResponse<byte[]> response = httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            try (var os = exchange.getResponseBody()) {
                os.write(response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, -1);
        }
    }
}
