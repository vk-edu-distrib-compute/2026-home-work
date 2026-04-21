package company.vk.edu.distrib.compute.kruchinina.sharding;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ClusterHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterHttpClient.class);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public void proxyRequest(String targetNode, HttpExchange originalExchange) throws IOException {
        String method = originalExchange.getRequestMethod();
        String path = originalExchange.getRequestURI().getPath();
        String query = originalExchange.getRequestURI().getQuery();
        String fullUrl = "http://" + targetNode + path + (query != null ? "?" + query : "");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Proxying {} request to {}", method, fullUrl);
        }

        try {
            byte[] body = originalExchange.getRequestBody().readAllBytes();
            HttpRequest request = buildHttpRequest(method, fullUrl, body, originalExchange);
            HttpResponse<byte[]> response = sendRequest(request);
            forwardResponse(response, originalExchange);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Proxy request interrupted for {}", fullUrl, e);
            throw new IOException("Proxy request interrupted", e);
        }
    }

    private HttpRequest buildHttpRequest(String method, String fullUrl, byte[] body, HttpExchange originalExchange) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .timeout(Duration.ofSeconds(30));

        for (Map.Entry<String, List<String>> header : originalExchange.getRequestHeaders().entrySet()) {
            String name = header.getKey();
            if (!isHopByHopHeader(name)) {
                for (String value : header.getValue()) {
                    requestBuilder.header(name, value);
                }
            }
        }

        switch (method) {
            case "GET":
                requestBuilder.GET();
                break;
            case "DELETE":
                requestBuilder.DELETE();
                break;
            case "PUT":
                requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                break;
            default:
                throw new IllegalArgumentException("Unsupported method: " + method);
        }
        return requestBuilder.build();
    }

    private HttpResponse<byte[]> sendRequest(HttpRequest request) throws IOException, InterruptedException {
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private void forwardResponse(HttpResponse<byte[]> response, HttpExchange originalExchange) throws IOException {
        originalExchange.getResponseHeaders().putAll(response.headers().map());
        originalExchange.sendResponseHeaders(response.statusCode(), response.body().length);
        try (OutputStream os = originalExchange.getResponseBody()) {
            os.write(response.body());
        }
        originalExchange.close();
    }

    private boolean isHopByHopHeader(String name) {
        return "Connection".equalsIgnoreCase(name)
                || "Keep-Alive".equalsIgnoreCase(name)
                || "Proxy-Authenticate".equalsIgnoreCase(name)
                || "Proxy-Authorization".equalsIgnoreCase(name)
                || "TE".equalsIgnoreCase(name)
                || "Trailer".equalsIgnoreCase(name)
                || "Transfer-Encoding".equalsIgnoreCase(name)
                || "Upgrade".equalsIgnoreCase(name);
    }
}
