package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpProxyClient implements ProxyClient {
    private final HttpClient httpClient;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    public HttpProxyClient() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
    }

    @Override
    public HttpResponse<byte[]> proxyGet(String endpoint, String key) throws IOException, InterruptedException {
        String url = buildUrl(endpoint, key);
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .timeout(TIMEOUT)
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    @Override
    public HttpResponse<Void> proxyPut(String endpoint, String key, byte[] value)
            throws IOException, InterruptedException {
        String url = buildUrl(endpoint, key);
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                .uri(URI.create(url))
                .timeout(TIMEOUT)
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public HttpResponse<Void> proxyDelete(String endpoint, String key)
            throws IOException, InterruptedException {
        String url = buildUrl(endpoint, key);
        HttpRequest request = HttpRequest.newBuilder()
                .DELETE()
                .uri(URI.create(url))
                .timeout(TIMEOUT)
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private String buildUrl(String endpoint, String key) {
        return endpoint + "/v0/entity?id=" + key;
    }
}

