package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import java.io.IOException;
import java.net.http.HttpResponse;

public interface ProxyClient {
    HttpResponse<byte[]> proxyGet(String endpoint, String key) throws IOException, InterruptedException;

    HttpResponse<Void> proxyPut(String endpoint, String key, byte[] value) throws IOException, InterruptedException;

    HttpResponse<Void> proxyDelete(String endpoint, String key) throws IOException, InterruptedException;

    void close();
}

