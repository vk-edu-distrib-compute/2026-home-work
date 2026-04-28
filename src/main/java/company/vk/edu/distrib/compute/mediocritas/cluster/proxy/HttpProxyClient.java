package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import company.vk.edu.distrib.compute.mediocritas.cluster.Node;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class HttpProxyClient implements ProxyClient {

    private final HttpClient httpClient;
    private final ProxyConfig config;

    public HttpProxyClient(ProxyConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(config.connectTimeout())
                .build();
    }

    public HttpProxyClient() {
        this(ProxyConfig.defaultConfig());
    }

    @Override
    public CompletableFuture<ProxyResponse<byte[]>> proxyGet(Node node, String key) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(buildUrl(node.httpEndpoint(), key)))
                .timeout(config.requestTimeout())
                .build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .<ProxyResponse<byte[]>>thenApply(response -> ProxyResponse.of(response.statusCode(), response.body()))
                .exceptionally(t -> ProxyResponse.error());
    }

    @Override
    public CompletableFuture<ProxyResponse<Void>> proxyPut(Node node, String key, byte[] value) {
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                .uri(URI.create(buildUrl(node.httpEndpoint(), key)))
                .timeout(config.requestTimeout())
                .build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .<ProxyResponse<Void>>thenApply(response -> ProxyResponse.of(response.statusCode(), null))
                .exceptionally(t -> ProxyResponse.error());
    }

    @Override
    public CompletableFuture<ProxyResponse<Void>> proxyDelete(Node node, String key) {
        HttpRequest request = HttpRequest.newBuilder()
                .DELETE()
                .uri(URI.create(buildUrl(node.httpEndpoint(), key)))
                .timeout(config.requestTimeout())
                .build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .<ProxyResponse<Void>>thenApply(response -> ProxyResponse.of(response.statusCode(), null))
                .exceptionally(t -> ProxyResponse.error());
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private String buildUrl(String endpoint, String key) {
        return endpoint + "/v0/entity?id=" + key;
    }
}
