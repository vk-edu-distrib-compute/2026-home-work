package company.vk.edu.distrib.compute.korjick.adapters.output;

import company.vk.edu.distrib.compute.korjick.adapters.input.http.Constants;
import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.StorageFailureException;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityGateway;
import company.vk.edu.distrib.compute.proto.korjick.EntityResponse;
import company.vk.edu.distrib.compute.proto.korjick.KeyRequest;
import company.vk.edu.distrib.compute.proto.korjick.ReactorGrpcKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.korjick.UpsertRequest;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GrpcEntityGateway implements EntityGateway {
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(2);

    private final Map<String, ReactorGrpcKVServiceGrpc.ReactorGrpcKVServiceStub> stubs;

    public GrpcEntityGateway() {
        this.stubs = new ConcurrentHashMap<>();
    }

    @Override
    public Entity getEntity(String endpoint, Entity.Key key) {
        EntityResponse response = blockResponse(
                stubFor(endpoint).get(KeyRequest.newBuilder().setKey(key.value()).build())
        );
        if (response.getStatus() == Constants.HTTP_STATUS_NOT_FOUND) {
            throw new EntityNotFoundException("Entity not found");
        }
        if (response.getStatus() != Constants.HTTP_STATUS_OK) {
            throw new StorageFailureException("Unexpected status from GET proxy: " + response.getStatus());
        }
        return new Entity(
                key,
                response.getBody().toByteArray(),
                response.getVersion(),
                response.getTombstone()
        );
    }

    @Override
    public void upsertEntity(String endpoint, Entity entity) {
        EntityResponse response = blockResponse(
                stubFor(endpoint).upsert(UpsertRequest.newBuilder()
                        .setKey(entity.key().value())
                        .setBody(ByteString.copyFrom(entity.body()))
                        .setVersion(entity.version())
                        .setTombstone(entity.deleted())
                        .build())
        );
        if (response.getStatus() != Constants.HTTP_STATUS_CREATED) {
            throw new StorageFailureException("Unexpected status from PUT proxy: " + response.getStatus());
        }
    }

    @Override
    public void deleteEntity(String endpoint, Entity.Key key) {
        EntityResponse response = blockResponse(
                stubFor(endpoint).delete(KeyRequest.newBuilder().setKey(key.value()).build())
        );
        if (response.getStatus() != Constants.HTTP_STATUS_ACCEPTED) {
            throw new StorageFailureException("Unexpected status from DELETE proxy: " + response.getStatus());
        }
    }

    private ReactorGrpcKVServiceGrpc.ReactorGrpcKVServiceStub stubFor(String endpoint) {
        return stubs.computeIfAbsent(endpoint, this::createStub);
    }

    private ReactorGrpcKVServiceGrpc.ReactorGrpcKVServiceStub createStub(String endpoint) {
        final var channel = Grpc.newChannelBuilder(endpoint,
                        InsecureChannelCredentials.create())
                .build();
        return ReactorGrpcKVServiceGrpc.newReactorStub(channel);
    }

    private EntityResponse blockResponse(Mono<EntityResponse> call) {
        EntityResponse response;
        try {
            response = call.block(RPC_TIMEOUT);
        } catch (RuntimeException e) {
            throw new StorageFailureException("Failed to proxy over gRPC", e);
        }
        if (response == null) {
            throw new StorageFailureException("Empty response from proxy");
        }
        return response;
    }
}
