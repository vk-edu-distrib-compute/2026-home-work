package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GrpcChannelRegistry implements AutoCloseable {
    private static final long TERMINATION_TIMEOUT_SECONDS = 10;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs = new ConcurrentHashMap<>();

    public GrpcChannelRegistry() {
    }

    public ReactorKVServiceGrpc.ReactorKVServiceStub getStub(String grpcEndpoint) {
        return stubs.computeIfAbsent(grpcEndpoint, endpoint -> {
            ManagedChannel channel = Grpc.newChannelBuilder(endpoint, InsecureChannelCredentials.create()).build();
            channels.put(endpoint, channel);
            return ReactorKVServiceGrpc.newReactorStub(channel);
        });
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                    channel.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        stubs.clear();
        channels.clear();
    }
}
