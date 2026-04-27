package company.vk.edu.distrib.compute.denchika.grpc;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.proto.denchika.DeleteRequest;
import company.vk.edu.distrib.compute.proto.denchika.DeleteResponse;
import company.vk.edu.distrib.compute.proto.denchika.GetRequest;
import company.vk.edu.distrib.compute.proto.denchika.GetResponse;
import company.vk.edu.distrib.compute.proto.denchika.InternalKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.denchika.PutRequest;
import company.vk.edu.distrib.compute.proto.denchika.PutResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

public class GrpcClusterServer {
    private static final Logger log = LoggerFactory.getLogger(GrpcClusterServer.class);

    private final int port;
    private final Dao<byte[]> dao;
    private Server server;

    public GrpcClusterServer(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    public void start() {
        try {
            server = ServerBuilder.forPort(port)
                .addService(new ServiceImpl())
                .build()
                .start();
            log.info("gRPC server started on port {}", port);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start gRPC server on port " + port, e);
        }
    }

    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    private final class ServiceImpl extends InternalKVServiceGrpc.InternalKVServiceImplBase {

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            try {
                byte[] data = dao.get(request.getId());
                responseObserver.onNext(GetResponse.newBuilder()
                    .setFound(true)
                    .setValue(ByteString.copyFrom(data))
                    .build());
            } catch (NoSuchElementException e) {
                responseObserver.onNext(GetResponse.newBuilder().setFound(false).build());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            responseObserver.onCompleted();
        }

        @Override
        public void upsert(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            try {
                dao.upsert(request.getKey(), request.getValue().toByteArray());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            responseObserver.onNext(PutResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            try {
                dao.delete(request.getId());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            responseObserver.onNext(DeleteResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
