package company.vk.edu.distrib.compute.expanse.client;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.expanse.dto.proto.DeleteEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.DeleteEntityResponse;
import company.vk.edu.distrib.compute.expanse.dto.proto.EntityServiceGrpc;
import company.vk.edu.distrib.compute.expanse.dto.proto.GetEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.GetEntityResponse;
import company.vk.edu.distrib.compute.expanse.dto.proto.PutEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.PutEntityResponse;
import company.vk.edu.distrib.compute.expanse.utils.ExceptionUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class GrpcClient {
    private static final Map<Integer, ManagedChannel> CHANNELS = new ConcurrentHashMap<>();

    private GrpcClient() {

    }

    public static GetEntityResponse getEntity(String entityId, int grpcPort) {
        try {
            GetEntityRequest request = GetEntityRequest.newBuilder().setId(entityId).build();
            EntityServiceGrpc.EntityServiceBlockingStub stub = build(grpcPort);
            return stub.getEntity(request);

        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                return GetEntityResponse.newBuilder()
                        .setCode(404)
                        .build();
            }
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    public static PutEntityResponse putEntity(String entityId, byte[] body, int grpcPort) {
        try {
            PutEntityRequest request = PutEntityRequest.newBuilder()
                    .setId(entityId)
                    .setBody(ByteString.copyFrom(body))
                    .build();
            EntityServiceGrpc.EntityServiceBlockingStub stub = build(grpcPort);
            return stub.putEntity(request);

        } catch (StatusRuntimeException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    public static DeleteEntityResponse deleteEntity(String entityId, int grpcPort) {
        try {
            DeleteEntityRequest request = DeleteEntityRequest.newBuilder()
                    .setId(entityId)
                    .build();
            EntityServiceGrpc.EntityServiceBlockingStub stub = build(grpcPort);
            return stub.deleteEntity(request);

        } catch (StatusRuntimeException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    private static EntityServiceGrpc.EntityServiceBlockingStub build(int grpcPort) {
        CHANNELS.putIfAbsent(grpcPort, ManagedChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build());
        return EntityServiceGrpc.newBlockingStub(CHANNELS.get(grpcPort));
    }
}
