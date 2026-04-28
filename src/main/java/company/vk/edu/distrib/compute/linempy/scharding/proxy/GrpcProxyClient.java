package company.vk.edu.distrib.compute.linempy.scharding.proxy;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import company.vk.edu.distrib.compute.linempy.*;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

public class GrpcProxyClient implements ProxyClient {
    private final ManagedChannel channel;
    private final ReactorKVServiceGrpc.ReactorKVServiceStub stub;

    public GrpcProxyClient(String host, int grpcPort) {
        this.channel = ManagedChannelBuilder.forAddress(host, grpcPort)
                .usePlaintext()
                .build();
        this.stub = ReactorKVServiceGrpc.newReactorStub(channel);
    }

    @Override
    public CompletableFuture<ProxyResponse> get(String targetUrl, String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return stub.get(Mono.just(request))
                .map(response -> {
                    if (response.getFound()) {
                        return ProxyResponse.of(200, response.getValue().toByteArray());
                    } else {
                        return ProxyResponse.of(404, new byte[0]);
                    }
                })
                .toFuture();
    }

    @Override
    public CompletableFuture<ProxyResponse> put(String targetUrl, String key, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        return stub.put(Mono.just(request))
                .map(response -> response.getSuccess()
                        ? ProxyResponse.of(201, new byte[0])
                        : ProxyResponse.of(500, new byte[0]))
                .toFuture();
    }

    @Override
    public CompletableFuture<ProxyResponse> delete(String targetUrl, String key) {
        DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
        return stub.delete(Mono.just(request))
                .map(response -> response.getSuccess()
                        ? ProxyResponse.of(202, new byte[0])
                        : ProxyResponse.of(500, new byte[0]))
                .toFuture();
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
