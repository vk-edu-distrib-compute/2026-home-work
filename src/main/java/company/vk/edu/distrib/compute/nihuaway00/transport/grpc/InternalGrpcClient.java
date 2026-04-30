package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.nihuaway00.app.InternalNodeClient;
import company.vk.edu.distrib.compute.nihuaway00.proto.Key;
import company.vk.edu.distrib.compute.nihuaway00.proto.KeyValue;
import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.proto.Response;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.NoSuchElementException;

public class InternalGrpcClient implements InternalNodeClient {
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(1);
    private final GrpcChannelRegistry channelRegistry;

    public InternalGrpcClient(GrpcChannelRegistry channelRegistry) {
        this.channelRegistry = channelRegistry;
    }

    @Override
    public byte[] get(String grpcEndpoint, String key, int ack) {
        try {
            Response response = requireStub(grpcEndpoint)
                    .get(Key.newBuilder()
                            .setKey(key)
                            .setAck(ack)
                            .build())
                    .block(RPC_TIMEOUT);
            if (response == null) {
                throw new IllegalStateException("Remote response is null");
            }
            return response.getValue().toByteArray();
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    @Override
    public void put(String grpcEndpoint, String key, byte[] value, int ack) {
        try {
            Response response = requireStub(grpcEndpoint)
                    .upsert(KeyValue.newBuilder()
                            .setKey(key)
                            .setValue(ByteString.copyFrom(value))
                            .setAck(ack)
                            .build())
                    .block(RPC_TIMEOUT);
            if (response == null) {
                throw new IllegalStateException("Remote response is null");
            }
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    @Override
    public void delete(String grpcEndpoint, String key, int ack) {
        try {
            Response response = requireStub(grpcEndpoint)
                    .delete(Key.newBuilder()
                            .setKey(key)
                            .setAck(ack)
                            .build())
                    .block(RPC_TIMEOUT);
            if (response == null) {
                throw new IllegalStateException("Remote response is null");
            }
        } catch (StatusRuntimeException e) {
            throw mapGrpcException(e);
        }
    }

    private ReactorKVServiceGrpc.ReactorKVServiceStub requireStub(String grpcEndpoint) {
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
