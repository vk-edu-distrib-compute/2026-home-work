package company.vk.edu.distrib.compute.kruchinina.sharding;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;

public class ClusterHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterHttpClient.class);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Set<String> RESTRICTED_HEADERS = Set.of(
            "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailer",
            "transfer-encoding", "upgrade", "host", "content-length"
    );

    public void proxyRequest(String targetNode, HttpExchange originalExchange) {
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
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Failed to proxy {} request to {}", method, fullUrl, e);
            }
            sendErrorResponse(originalExchange, "Proxy error: " + e.getMessage());
        }
    }

    private HttpRequest buildHttpRequest(String method, String fullUrl,
                                         byte[] body, HttpExchange originalExchange) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .timeout(Duration.ofSeconds(30));

        for (Map.Entry<String, List<String>> header : originalExchange.getRequestHeaders().entrySet()) {
            String name = header.getKey();
            if (!isRestrictedHeader(name)) {
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

    private void forwardResponse(HttpResponse<byte[]> response, HttpExchange originalExchange)
            throws IOException {
        originalExchange.getResponseHeaders().putAll(response.headers().map());
        originalExchange.sendResponseHeaders(response.statusCode(), response.body().length);
        try (OutputStream os = originalExchange.getResponseBody()) {
            os.write(response.body());
        }
        originalExchange.close();
    }

    private void sendErrorResponse(HttpExchange exchange, String message) {
        try (AutoCloseable ignored = exchange::close) {
            byte[] body = message.getBytes();
            exchange.sendResponseHeaders(500, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        } catch (Exception ex) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Failed to send error response", ex);
            }
        }
    }

    private boolean isRestrictedHeader(String name) {
        return RESTRICTED_HEADERS.contains(name.toLowerCase());
    }
}
