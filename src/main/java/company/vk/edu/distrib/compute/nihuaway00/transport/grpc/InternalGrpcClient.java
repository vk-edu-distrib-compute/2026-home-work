package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.nihuaway00.app.InternalNodeClient;
import company.vk.edu.distrib.compute.nihuaway00.proto.Key;
import company.vk.edu.distrib.compute.nihuaway00.proto.KeyValue;
import company.vk.edu.distrib.compute.nihuaway00.proto.KVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.proto.Response;
import io.grpc.StatusRuntimeException;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class InternalGrpcClient implements InternalNodeClient {
    private static final long RPC_TIMEOUT_SECONDS = 1L;
    private final GrpcChannelRegistry channelRegistry;

    public InternalGrpcClient(GrpcChannelRegistry channelRegistry) {
        this.channelRegistry = channelRegistry;
    }

    @Override
    public byte[] get(String grpcEndpoint, String key, int ack) {
        try {
            Response response = requireStub(grpcEndpoint)
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get(Key.newBuilder()
                            .setKey(key)
                            .setAck(ack)
                            .build());
            return response.getValue().toByteArray();
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    @Override
    public void put(String grpcEndpoint, String key, byte[] value, int ack) {
        try {
            requireStub(grpcEndpoint)
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .upsert(KeyValue.newBuilder()
                            .setKey(key)
                            .setValue(ByteString.copyFrom(value))
                            .setAck(ack)
                            .build());
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    @Override
    public void delete(String grpcEndpoint, String key, int ack) {
        try {
            requireStub(grpcEndpoint)
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .delete(Key.newBuilder()
                            .setKey(key)
                            .setAck(ack)
                            .build());
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    private KVServiceGrpc.KVServiceBlockingStub requireStub(String grpcEndpoint) {
        return channelRegistry.getStub(grpcEndpoint);
    }

    private RuntimeException mapGrpcException(StatusRuntimeException exception) {
        return switch (exception.getStatus().getCode()) {
            case NOT_FOUND -> new NoSuchElementException(exception.getMessage());
            case INVALID_ARGUMENT -> new IllegalArgumentException(exception.getMessage());
            default -> new IllegalStateException(exception.getMessage(), exception);
        };
    }
}
