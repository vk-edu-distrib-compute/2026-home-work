package company.vk.edu.distrib.compute.korjick.adapters.input.grpc;

import company.vk.edu.distrib.compute.korjick.adapters.input.http.Constants;
import company.vk.edu.distrib.compute.korjick.core.application.SingleNodeCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.proto.korjick.EntityResponse;
import company.vk.edu.distrib.compute.proto.korjick.KeyRequest;
import company.vk.edu.distrib.compute.proto.korjick.ReactorGrpcKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.korjick.UpsertRequest;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class CakeGrpcServer extends ReactorGrpcKVServiceGrpc.GrpcKVServiceImplBase {

    private static final int PORT_PREFIX = 50000;
    private final Server server;
    private final SingleNodeCoordinator coordinator;
    private final String endpoint;

    public CakeGrpcServer(String host, int port,
                          SingleNodeCoordinator coordinator) {
        super();
        this.coordinator = coordinator;

        int grpcPort = resolveGrpcPort(port);
        this.server = NettyServerBuilder
                .forAddress(new InetSocketAddress(host, grpcPort))
                .addService(this)
                .addService(ProtoReflectionService.newInstance())
                .build();
        this.endpoint = resolveEndpoint(host, port);
    }

    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void stop() {
        server.shutdown();
        try {
            if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    @Override
    public Mono<EntityResponse> get(Mono<KeyRequest> request) {
        return request.map(req -> {
            try {
                Entity entity = coordinator.get(new Entity.Key(req.getKey()));
                return EntityResponse.newBuilder()
                        .setStatus(Constants.HTTP_STATUS_OK)
                        .setBody(ByteString.copyFrom(entity.body()))
                        .setVersion(entity.version())
                        .setTombstone(entity.deleted())
                        .build();
            } catch (Exception e) {
                return handleError(e);
            }
        });
    }

    @Override
    public Mono<EntityResponse> upsert(Mono<UpsertRequest> request) {
        return request.map(req -> {
            try {
                Entity entity = new Entity(
                        new Entity.Key(req.getKey()),
                        req.getBody().toByteArray(),
                        req.getVersion(),
                        req.getTombstone()
                );
                coordinator.upsert(entity);
                return EntityResponse.newBuilder()
                        .setStatus(Constants.HTTP_STATUS_CREATED)
                        .build();
            } catch (Exception e) {
                return handleError(e);
            }
        });
    }

    @Override
    public Mono<EntityResponse> delete(Mono<KeyRequest> request) {
        return request.map(req -> {
            try {
                coordinator.delete(new Entity.Key(req.getKey()));
                return EntityResponse.newBuilder()
                        .setStatus(Constants.HTTP_STATUS_ACCEPTED)
                        .build();
            } catch (Exception e) {
                return handleError(e);
            }
        });
    }

    public static String resolveEndpoint(String host, int port) {
        return String.format("%s:%d", host, resolveGrpcPort(port));
    }

    private static int resolveGrpcPort(int port) {
        return (PORT_PREFIX + port) % Character.MAX_VALUE;
    }

    private EntityResponse handleError(Throwable e) {
        return switch (e) {
            case IllegalArgumentException _ ->
                    EntityResponse.newBuilder().setStatus(Constants.HTTP_STATUS_BAD_REQUEST).build();
            case EntityNotFoundException _ ->
                    EntityResponse.newBuilder().setStatus(Constants.HTTP_STATUS_NOT_FOUND).build();
            case null, default -> EntityResponse.newBuilder().setStatus(Constants.HTTP_STATUS_INTERNAL_ERROR).build();
        };
    }
}
