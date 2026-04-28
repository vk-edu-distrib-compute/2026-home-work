package company.vk.edu.distrib.compute.wedwincode.sharded.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Empty;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.proto.wedwincode.DeleteRequest;
import company.vk.edu.distrib.compute.proto.wedwincode.GetRequest;
import company.vk.edu.distrib.compute.proto.wedwincode.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.wedwincode.Response;
import company.vk.edu.distrib.compute.proto.wedwincode.UpsertRequest;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import io.grpc.Status;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;

public class GrpcInternalService extends ReactorKVServiceGrpc.KVServiceImplBase {

    private final Dao<DaoRecord> dao;

    public GrpcInternalService(Dao<DaoRecord> dao) {
        super();
        this.dao = dao;
    }

    @Override
    public Mono<Response> get(GetRequest request) {
        try {
            DaoRecord record = dao.get(request.getKey());
            return Mono.just(Response.newBuilder()
                    .setValue(BytesValue.of(ByteString.copyFrom(record.data())))
                    .build());
        } catch (NoSuchElementException e) {
            return Mono.error(Status.NOT_FOUND.asRuntimeException());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public Mono<Empty> upsert(UpsertRequest request) {
        try {
            dao.upsert(
                    request.getKey(),
                    DaoRecord.buildCreated(request.getValue().toByteArray())
            );
            return Mono.just(Empty.getDefaultInstance());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public Mono<Empty> delete(DeleteRequest request) {
        try {
            dao.delete(request.getKey());
            return Mono.just(Empty.getDefaultInstance());
        } catch (Exception e) {
            return Mono.error(Status.INTERNAL.asRuntimeException());
        }
    }
}
