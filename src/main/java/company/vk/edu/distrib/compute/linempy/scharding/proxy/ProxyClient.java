package company.vk.edu.distrib.compute.linempy.scharding.proxy;

import java.util.concurrent.CompletableFuture;

public interface ProxyClient {
    CompletableFuture<ProxyResponse> get(String targetUrl, String key);

    CompletableFuture<ProxyResponse> put(String targetUrl, String key, byte[] value);

    CompletableFuture<ProxyResponse> delete(String targetUrl, String key);

    void close();
}
