package company.vk.edu.distrib.compute.artttnik;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.artttnik.grpc.InternalKvServiceGrpc;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyRequest;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class InternalGrpcService extends InternalKvServiceGrpc.InternalKvServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(InternalGrpcService.class);
    private final MyReplicatedKVService owner;

    InternalGrpcService(MyReplicatedKVService owner) {
        super();
        this.owner = owner;
    }

    @Override
    public void proxy(ProxyRequest request, StreamObserver<ProxyResponse> responseObserver) {
        try {
            int ack = request.getHasAck() ? request.getAck() : LocalRequestHandler.DEFAULT_ACK;
            ProxyResult result = owner.handleLocalRequest(
                    request.getMethod(),
                    request.getId(),
                    ack,
                    request.getBody().toByteArray()
            );

            ProxyResponse response = ProxyResponse.newBuilder()
                    .setStatusCode(result.statusCode())
                    .setBody(ByteString.copyFrom(result.body()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("gRPC proxy handler error", e);
            responseObserver.onNext(ProxyResponse.newBuilder().setStatusCode(500).build());
            responseObserver.onCompleted();
        }
    }
}
