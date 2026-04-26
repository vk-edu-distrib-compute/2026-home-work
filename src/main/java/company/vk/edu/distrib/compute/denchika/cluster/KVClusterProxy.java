package company.vk.edu.distrib.compute.denchika.cluster;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class KVClusterProxy {

    private final HttpClient client = HttpClient.newHttpClient();

    public void proxy(HttpExchange ex, String target) throws IOException {
        URI uri = ex.getRequestURI();
        String query = uri.getRawQuery();
        String method = ex.getRequestMethod();

        URI targetUri = URI.create(target + "/v0/entity?" + query);

        byte[] body = ex.getRequestBody().readAllBytes();

        HttpRequest.BodyPublisher publisher =
                body.length > 0
                        ? HttpRequest.BodyPublishers.ofByteArray(body)
                        : HttpRequest.BodyPublishers.noBody();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(targetUri)
                .method(method, publisher)
                .build();

        HttpResponse<byte[]> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        byte[] respBody = response.body();
        int status = response.statusCode();

        if (respBody == null) {
            ex.sendResponseHeaders(status, -1);
            return;
        }

        ex.sendResponseHeaders(status, respBody.length);
        ex.getResponseBody().write(respBody);
    }
}
