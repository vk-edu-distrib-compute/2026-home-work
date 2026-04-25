package company.vk.edu.distrib.compute.vitos23.internal;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.proto.vitos23.KeyRequest;
import company.vk.edu.distrib.compute.proto.vitos23.ReactorInternalKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.vitos23.UpsertRequest;
import company.vk.edu.distrib.compute.vitos23.util.ByteArrayKey;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

public class InternalGrpcKVClient {

    private static final int TIMEOUT_SECONDS = 10;

    private final ManagedChannel channel;
    private final ReactorInternalKVServiceGrpc.ReactorInternalKVServiceStub internalKVServiceStub;

    public InternalGrpcKVClient(String grpcEndpoint) {
        channel = Grpc.newChannelBuilder(grpcEndpoint, InsecureChannelCredentials.create()).build();
        internalKVServiceStub = ReactorInternalKVServiceGrpc.newReactorStub(channel);
    }

    public void stop() throws InterruptedException {
        channel.shutdown();
        channel.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @SuppressWarnings("all") // PMD.NullAssignment for Codacy
    public Mono<ByteArrayKey> get(String key) {
        return internalKVServiceStub.get(getKeyRequest(key))
                .map(response -> new ByteArrayKey(
                        response.hasValue() ? response.getValue().getValue().toByteArray() : null
                ));
    }

    public Mono<Void> upsert(String key, byte[] value) {
        UpsertRequest request = UpsertRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        return internalKVServiceStub.upsert(request).then();
    }

    public Mono<Void> delete(String key) {
        return internalKVServiceStub.delete(getKeyRequest(key)).then();
    }

    private static KeyRequest getKeyRequest(String key) {
        return KeyRequest.newBuilder().setKey(key).build();
    }

}
