package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

final class CeFelloClusterProxyClient {
    private static final Logger log = LoggerFactory.getLogger(CeFelloClusterProxyClient.class);

    private final String localEndpoint;
    private final HttpClient httpClient;

    CeFelloClusterProxyClient(String localEndpoint) {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .build();
    }

    void forward(HttpExchange exchange, String ownerEndpoint, String requestMethod) throws IOException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(CeFelloClusterHttpHelper.proxyUri(exchange, ownerEndpoint))
                .timeout(Duration.ofSeconds(2))
                .header(
                        CeFelloClusterHttpHelper.PROXY_HEADER,
                        CeFelloClusterHttpHelper.PROXY_HEADER_VALUE
                );

        switch (requestMethod) {
            case CeFelloClusterHttpHelper.GET_METHOD -> requestBuilder.GET();
            case CeFelloClusterHttpHelper.PUT_METHOD -> requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(
                    exchange.getRequestBody().readAllBytes()
            ));
            case CeFelloClusterHttpHelper.DELETE_METHOD -> requestBuilder.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + requestMethod);
        }

        try {
            HttpRequest request = requestBuilder.build();
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            CeFelloClusterHttpHelper.sendBody(exchange, response.statusCode(), response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            CeFelloClusterHttpHelper.sendEmpty(exchange, 503);
        } catch (IOException e) {
            log.warn("Failed to proxy request from {} to {}", localEndpoint, ownerEndpoint, e);
            CeFelloClusterHttpHelper.sendEmpty(exchange, 503);
        }
    }
}
