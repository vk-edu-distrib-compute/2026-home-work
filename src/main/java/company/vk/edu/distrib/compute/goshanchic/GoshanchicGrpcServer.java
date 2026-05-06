package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternalServiceGrpc;
import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternal.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public final class GoshanchicGrpcServer {
    private final Server server;
    private final InMemoryDao dao;

    public GoshanchicGrpcServer(int grpcPort, InMemoryDao dao) {
        this.server = ServerBuilder.forPort(grpcPort)
                .addService(new KVInternalServiceImpl())
                .build();
        this.dao = dao;
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() {
        server.shutdown();
    }

    private final class KVInternalServiceImpl extends KVInternalServiceGrpc.KVInternalServiceImplBase {

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            try {
                byte[] value = dao.get(request.getId());
                responseObserver.onNext(GetResponse.newBuilder()
                        .setStatus(200)
                        .setValue(com.google.protobuf.ByteString.copyFrom(value))
                        .build());
            } catch (Exception e) {
                responseObserver.onNext(GetResponse.newBuilder()
                        .setStatus(404)
                        .build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            try {
                dao.upsert(request.getId(), request.getValue().toByteArray());
                responseObserver.onNext(PutResponse.newBuilder()
                        .setStatus(201)
                        .build());
            } catch (Exception e) {
                responseObserver.onNext(PutResponse.newBuilder()
                        .setStatus(500)
                        .build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            try {
                dao.delete(request.getId());
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setStatus(202)
                        .build());
            } catch (Exception e) {
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setStatus(500)
                        .build());
            }
            responseObserver.onCompleted();
        }
    }
}
