package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import company.vk.edu.distrib.compute.mediocritas.KvServiceGrpc;
import company.vk.edu.distrib.compute.mediocritas.service.GrpcKvByteService;
import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GrpcProxyClient implements ProxyClient {

    private final Map<String, KvServiceGrpc.KvServiceBlockingStub> stubs = new ConcurrentHashMap<>();


    @Override
    public HttpResponse<byte[]> proxyGet(String endpoint, String key) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public HttpResponse<Void> proxyPut(String endpoint, String key, byte[] value) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public HttpResponse<Void> proxyDelete(String endpoint, String key) throws IOException, InterruptedException {
        return null;
    }

    private KvServiceGrpc.KvServiceBlockingStub getStub(String endpoint) {
        stubs.computeIfAbsent(

        )
    }

    //todo
    // create record Node(Sting host, int httpPort, int grpcPort)
    // store Node list in cluster
    // pass Node to proxies
    // timeouts to proxy configuration

    @Override
    public void close() {
        // not resources to close yet
    }
}
