package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.proto.Key;
import company.vk.edu.distrib.compute.nihuaway00.proto.KeyValue;
import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.proto.Response;
import company.vk.edu.distrib.compute.nihuaway00.replication.InsufficientReplicasException;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.protobuf.services.ProtoReflectionService;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class InternalGrpcService extends ReactorKVServiceGrpc.KVServiceImplBase {
    private final KVCommandService commandService;
    private Server server;

    public InternalGrpcService(KVCommandService commandService) {
        this.commandService = commandService;
    }

    public void newGrpcServer(int port) throws InterruptedException {
        if (server != null) {
            server.shutdown();
            server.awaitTermination(1, TimeUnit.SECONDS);
        }

        this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(this)
                .addService(ProtoReflectionService.newInstance())
                .build();

    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() {
        server.shutdown();
    }

    public boolean isShutdown() {
        return server.isShutdown();
    }

    public boolean isTerminated() {
        return server.isTerminated();
    }

    @Override
    public Mono<Response> get(Key request) {
        try {
            byte[] data = commandService.handleGetEntity(request.getKey(),
                    request.getAck());
            return Mono.just(Response.newBuilder()
                    .setStatus(200)
                    .setValue(ByteString.copyFrom(data))
                    .build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (InsufficientReplicasException e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }

    }

    @Override
    public Mono<Response> upsert(KeyValue request) {
        try {
            commandService.handlePutEntity(request.getKey(), request.getValue().toByteArray(), request.getAck());
            return Mono.just(Response.newBuilder().setStatus(201).build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (InsufficientReplicasException e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public Mono<Response> delete(Key request) {
        try {
            commandService.handleDeleteEntity(request.getKey(), request.getAck());
            return Mono.just(Response.newBuilder().setStatus(202).build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            return Mono.error(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (InsufficientReplicasException e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

}
