package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import company.vk.edu.distrib.compute.mediocritas.cluster.Node;

import java.util.concurrent.CompletableFuture;

public interface ProxyClient {

    CompletableFuture<ProxyResponse<byte[]>> proxyGet(Node node, String key);

    CompletableFuture<ProxyResponse<Void>> proxyPut(Node node, String key, byte[] value);

    CompletableFuture<ProxyResponse<Void>> proxyDelete(Node node, String key);

    void close();
}
