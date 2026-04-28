package company.vk.edu.distrib.compute.arseniy90.service;

import com.google.protobuf.ByteString;

import company.vk.edu.distrib.compute.arseniy90.model.Response;
import company.vk.edu.distrib.compute.arseniy90.replication.GrpcReplicaClient;
import company.vk.edu.distrib.compute.proto.arseniy90.DeleteRequest;
import company.vk.edu.distrib.compute.proto.arseniy90.GetRequest;
import company.vk.edu.distrib.compute.proto.arseniy90.GrpcKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.arseniy90.UpsertRequest;
import company.vk.edu.distrib.compute.proto.arseniy90.InternalResponse;
import io.grpc.stub.StreamObserver;

public class GrpcKVServiceImpl extends GrpcKVServiceGrpc.GrpcKVServiceImplBase {
    private final GrpcReplicaClient grpcClient;

    public GrpcKVServiceImpl(GrpcReplicaClient grpcClient) {
        super();
        this.grpcClient = grpcClient;
    }

    @Override
   public void get(GetRequest request, StreamObserver<InternalResponse> responseObserver) {
        Response localResp = grpcClient.processLocalRequest("GET", request.getKey(), null);
        responseObserver.onNext(mapToProto(localResp));
        responseObserver.onCompleted();
    }

    @Override
    public void upsert(UpsertRequest request, StreamObserver<InternalResponse> responseObserver) {
        Response localResp = grpcClient.processLocalRequest("PUT", request.getKey(), request.getValue().toByteArray());
        responseObserver.onNext(mapToProto(localResp));
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<InternalResponse> responseObserver) {
        Response localResp = grpcClient.processLocalRequest("DELETE", request.getKey(), null);
        responseObserver.onNext(mapToProto(localResp));
        responseObserver.onCompleted();
    }

    private InternalResponse mapToProto(Response resp) {
        var builder = InternalResponse.newBuilder().setStatus(resp.status());
        if (resp.body() != null) {
            builder.setValue(ByteString.copyFrom(resp.body()));
        }
        return builder.build();
    }
}

