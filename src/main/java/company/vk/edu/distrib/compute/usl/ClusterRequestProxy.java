package company.vk.edu.distrib.compute.usl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

final class ClusterRequestProxy {
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final HttpClient httpClient;

    ClusterRequestProxy(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    ProxyResponse proxy(String endpoint, String method, URI requestUri, byte[] requestBody)
        throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(targetUri(endpoint, requestUri))
            .timeout(PROXY_TIMEOUT)
            .method(method, requestPublisher(method, requestBody))
            .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return new ProxyResponse(response.statusCode(), response.body());
    }

    private static URI targetUri(String endpoint, URI requestUri) {
        StringBuilder target = new StringBuilder(endpoint).append(requestUri.getRawPath());
        if (requestUri.getRawQuery() != null) {
            target.append('?').append(requestUri.getRawQuery());
        }
        return URI.create(target.toString());
    }

    private static HttpRequest.BodyPublisher requestPublisher(String method, byte[] requestBody) {
        return switch (method) {
            case "GET", "DELETE" -> HttpRequest.BodyPublishers.noBody();
            default -> HttpRequest.BodyPublishers.ofByteArray(requestBody);
        };
    }

    record ProxyResponse(int statusCode, byte[] body) {
    }
}
