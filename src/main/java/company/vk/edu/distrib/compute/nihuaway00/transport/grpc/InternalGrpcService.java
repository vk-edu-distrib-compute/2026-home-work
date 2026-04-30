package company.vk.edu.distrib.compute.nihuaway00.transport.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.proto.Key;
import company.vk.edu.distrib.compute.nihuaway00.proto.KeyValue;
import company.vk.edu.distrib.compute.nihuaway00.proto.KVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.proto.Response;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.NoSuchElementException;

public class InternalGrpcService extends KVServiceGrpc.KVServiceImplBase {
    private final KVCommandService commandService;

    public InternalGrpcService(KVCommandService commandService) {
        super();
        this.commandService = commandService;
    }

    @Override
    public void get(Key request, StreamObserver<Response> responseObserver) {
        try {
            byte[] data = commandService.handleGetEntity(request.getKey(),
                    request.getAck());
            responseObserver.onNext(Response.newBuilder()
                    .setStatus(200)
                    .setValue(ByteString.copyFrom(data))
                    .build());
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public void upsert(KeyValue request, StreamObserver<Response> responseObserver) {
        try {
            commandService.handlePutEntity(request.getKey(), request.getValue().toByteArray(), request.getAck());
            responseObserver.onNext(Response.newBuilder().setStatus(201).build());
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public void delete(Key request, StreamObserver<Response> responseObserver) {
        try {
            commandService.handleDeleteEntity(request.getKey(), request.getAck());
            responseObserver.onNext(Response.newBuilder().setStatus(202).build());
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }
}
