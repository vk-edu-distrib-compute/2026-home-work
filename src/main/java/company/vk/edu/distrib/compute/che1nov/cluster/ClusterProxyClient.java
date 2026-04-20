package company.vk.edu.distrib.compute.che1nov.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

public class ClusterProxyClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);

    private final HttpClient httpClient;

    public ClusterProxyClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    @SuppressWarnings("PMD.UseObjectForClearerAPI")
    public HttpResponse<byte[]> forward(
            String method,
            String targetEndpoint,
            String path,
            String key,
            byte[] body
    ) throws IOException, InterruptedException {
        validateMethod(method);
        validateTargetEndpoint(targetEndpoint);
        validatePath(path);
        validateKey(key);

        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create(targetEndpoint + path + "?id=" + encodedKey);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(REQUEST_TIMEOUT);

        switch (method) {
            case "GET" -> requestBuilder.GET();
            case "PUT" -> requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body == null ? new byte[0] : body));
            case "DELETE" -> requestBuilder.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }

        return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static void validateMethod(String method) {
        if (method == null || method.isBlank()) {
            throw new IllegalArgumentException("method must not be null or blank");
        }
    }

    private static void validateTargetEndpoint(String targetEndpoint) {
        if (targetEndpoint == null || targetEndpoint.isBlank()) {
            throw new IllegalArgumentException("targetEndpoint must not be null or blank");
        }
    }

    private static void validatePath(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path must not be null or blank");
        }
    }

    private static void validateKey(String key) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
    }
}
