package company.vk.edu.distrib.compute.vitos23.internal;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Empty;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.proto.vitos23.GetResponse;
import company.vk.edu.distrib.compute.proto.vitos23.KeyRequest;
import company.vk.edu.distrib.compute.proto.vitos23.ReactorInternalKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.vitos23.UpsertRequest;
import company.vk.edu.distrib.compute.vitos23.exception.ServerException;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.NoSuchElementException;

public class InternalGrpcKVService extends ReactorInternalKVServiceGrpc.InternalKVServiceImplBase implements KVService {

    private final Server grpcServer;
    private final Dao<byte[]> dao;

    public InternalGrpcKVService(int port, Dao<byte[]> dao) {
        super();
        this.grpcServer = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(this)
                .build();
        this.dao = dao;
    }

    @Override
    public void start() {
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new ServerException(e);
        }
    }

    @Override
    public void stop() {
        grpcServer.shutdown();
        try {
            grpcServer.awaitTermination();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Mono<GetResponse> get(KeyRequest request) {
        try {
            byte[] value = dao.get(request.getKey());
            var response = GetResponse.newBuilder()
                    .setValue(BytesValue.of(ByteString.copyFrom(value)))
                    .build();
            return Mono.just(response);
        } catch (NoSuchElementException e) {
            return Mono.just(GetResponse.newBuilder().build());
        } catch (IOException e) {
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Empty> upsert(UpsertRequest request) {
        try {
            dao.upsert(request.getKey(), request.getValue().toByteArray());
        } catch (IOException e) {
            return Mono.error(e);
        }
        return Mono.just(Empty.getDefaultInstance());
    }

    @Override
    public Mono<Empty> delete(KeyRequest request) {
        try {
            dao.delete(request.getKey());
        } catch (IOException e) {
            return Mono.error(e);
        }
        return Mono.just(Empty.getDefaultInstance());
    }
}
