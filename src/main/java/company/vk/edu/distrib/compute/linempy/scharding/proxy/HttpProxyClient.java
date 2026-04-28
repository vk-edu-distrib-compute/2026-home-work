package company.vk.edu.distrib.compute.linempy.scharding.proxy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class HttpProxyClient implements ProxyClient {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private final HttpClient client = HttpClient.newHttpClient();

    @Override
    public CompletableFuture<ProxyResponse> get(String targetUrl, String key) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl + "/v0/entity?id=" + key))
                .timeout(TIMEOUT)
                .GET()
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(resp -> ProxyResponse.of(resp.statusCode(), resp.body()));
    }

    @Override
    public CompletableFuture<ProxyResponse> put(String targetUrl, String key, byte[] value) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl + "/v0/entity?id=" + key))
                .timeout(TIMEOUT)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(resp -> ProxyResponse.of(resp.statusCode(), resp.body()));
    }

    @Override
    public CompletableFuture<ProxyResponse> delete(String targetUrl, String key) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl + "/v0/entity?id=" + key))
                .timeout(TIMEOUT)
                .DELETE()
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(resp -> ProxyResponse.of(resp.statusCode(), resp.body()));
    }

    @Override
    public void close() {
        // nothing
    }
}
