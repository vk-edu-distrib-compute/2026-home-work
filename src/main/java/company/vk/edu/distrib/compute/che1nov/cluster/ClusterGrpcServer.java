package company.vk.edu.distrib.compute.che1nov.cluster;

import company.vk.edu.distrib.compute.che1nov.KVServiceImpl;
import company.vk.edu.distrib.compute.che1nov.grpc.InternalTransportServiceGrpc;
import company.vk.edu.distrib.compute.che1nov.grpc.ProxyRequest;
import company.vk.edu.distrib.compute.che1nov.grpc.ProxyResponse;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

public class ClusterGrpcServer implements Closeable {
    private final Server grpcServer;

    public ClusterGrpcServer(int grpcPort, KVServiceImpl service) {
        this.grpcServer = NettyServerBuilder.forPort(grpcPort)
                .addService(new InternalTransportService(service))
                .build();
    }

    public void start() {
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start gRPC server", e);
        }
    }

    @Override
    public void close() {
        grpcServer.shutdownNow();
        try {
            grpcServer.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class InternalTransportService
            extends InternalTransportServiceGrpc.InternalTransportServiceImplBase {
        private final KVServiceImpl service;

        private InternalTransportService(KVServiceImpl service) {
            super();
            this.service = service;
        }

        @Override
        public void forward(ProxyRequest request, StreamObserver<ProxyResponse> responseObserver) {
            KVServiceImpl.InternalResponse response = service.handleInternalTransport(
                    request.getMethod(),
                    request.getKey(),
                    request.getBody().toByteArray()
            );
            responseObserver.onNext(ProxyResponse.newBuilder()
                    .setStatusCode(response.statusCode())
                    .setBody(com.google.protobuf.ByteString.copyFrom(response.body()))
                    .build());
            responseObserver.onCompleted();
        }
    }
}
