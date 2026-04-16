package company.vk.edu.distrib.compute.che1nov.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class ClusterProxyClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    public static final String INTERNAL_REQUEST_HEADER = "X-Che1nov-Proxy-Request";
    public static final String INTERNAL_REQUEST_VALUE = "true";

    private final HttpClient httpClient;

    public ClusterProxyClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    public HttpResponse<byte[]> forward(
            String method,
            String targetEndpoint,
            String path,
            Map<String, String> queryParams,
            boolean internalRequest,
            byte[] body
    ) throws IOException, InterruptedException {
        validateMethod(method);
        validateTargetEndpoint(targetEndpoint);
        validatePath(path);
        validateQueryParams(queryParams);

        URI uri = URI.create(targetEndpoint + path + "?" + encodeQueryParams(queryParams));

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(REQUEST_TIMEOUT);
        if (internalRequest) {
            requestBuilder.header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE);
        }

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

    private static void validateQueryParams(Map<String, String> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            throw new IllegalArgumentException("queryParams must not be null or empty");
        }
    }

    private static String encodeQueryParams(Map<String, String> queryParams) {
        StringJoiner joiner = new StringJoiner("&");
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (Objects.isNull(key) || key.isBlank()) {
                throw new IllegalArgumentException("query parameter key must not be null or blank");
            }

            String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
            String encodedValue = URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
            joiner.add(encodedKey + "=" + encodedValue);
        }
        return joiner.toString();
    }
}
