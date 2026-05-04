package company.vk.edu.distrib.compute.che1nov.cluster;

import company.vk.edu.distrib.compute.che1nov.grpc.InternalTransportServiceGrpc;
import company.vk.edu.distrib.compute.che1nov.grpc.ProxyRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class ClusterProxyClient implements Closeable {
    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalTransportServiceGrpc.InternalTransportServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();

    public ProxyResponse forward(
            String method,
            String targetEndpoint,
            String key,
            byte[] body
    ) {
        validateMethod(method);
        validateEndpoint(targetEndpoint);
        validateKey(key);

        NodeEndpoint nodeEndpoint = NodeEndpoint.parse(targetEndpoint);
        String grpcTarget = nodeEndpoint.grpcHost() + ":" + nodeEndpoint.grpcPort();

        InternalTransportServiceGrpc.InternalTransportServiceBlockingStub stub =
                stubs.computeIfAbsent(grpcTarget, this::createStub);

        ProxyRequest request = ProxyRequest.newBuilder()
                .setMethod(method)
                .setKey(key)
                .setBody(com.google.protobuf.ByteString.copyFrom(body == null ? new byte[0] : body))
                .build();

        var response = stub.forward(request);
        return new ProxyResponse(response.getStatusCode(), response.getBody().toByteArray());
    }

    @Override
    public void close() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            ManagedChannel channel = entry.getValue();
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
        stubs.clear();
    }

    private InternalTransportServiceGrpc.InternalTransportServiceBlockingStub createStub(String grpcTarget) {
        ManagedChannel channel = channels.computeIfAbsent(grpcTarget, this::createChannel);
        return InternalTransportServiceGrpc.newBlockingStub(channel);
    }

    private ManagedChannel createChannel(String grpcTarget) {
        return ManagedChannelBuilder.forTarget(grpcTarget)
                .usePlaintext()
                .build();
    }

    private static void validateMethod(String method) {
        if (method == null || method.isBlank()) {
            throw new IllegalArgumentException("method must not be null or blank");
        }
    }

    private static void validateEndpoint(String targetEndpoint) {
        if (targetEndpoint == null || targetEndpoint.isBlank()) {
            throw new IllegalArgumentException("targetEndpoint must not be null or blank");
        }
    }

    private static void validateKey(String key) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
    }

    public record ProxyResponse(int statusCode, byte[] body) {
    }
}
