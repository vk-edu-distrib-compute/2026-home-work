package company.vk.edu.distrib.compute.linempy.scharding;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.linempy.*;
import io.grpc.Status;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;

public class LinempyGrpcService extends ReactorKVServiceGrpc.KVServiceImplBase {

    private final Dao<byte[]> dao;

    public LinempyGrpcService(Dao<byte[]> dao) {
        super();
        this.dao = dao;
    }

    @Override
    public Mono<GetResponse> get(Mono<GetRequest> request) {
        return request.map(req -> {
            try {
                byte[] value = dao.get(req.getKey());
                return GetResponse.newBuilder()
                        .setFound(true)
                        .setValue(ByteString.copyFrom(value))
                        .build();
            } catch (NoSuchElementException e) {
                return GetResponse.newBuilder()
                        .setFound(false)
                        .setValue(ByteString.EMPTY)
                        .build();
            } catch (Exception e) {
                throw Status.INTERNAL.withDescription(e.getMessage())
                        .withCause(e)
                        .asRuntimeException();
            }
        });
    }

    @Override
    public Mono<PutResponse> put(Mono<PutRequest> request) {
        return request.map(req -> {
            try {
                dao.upsert(req.getKey(), req.getValue().toByteArray());
                return PutResponse.newBuilder().setSuccess(true).build();
            } catch (Exception e) {
                return PutResponse.newBuilder().setSuccess(false).build();
            }
        });
    }

    @Override
    public Mono<DeleteResponse> delete(Mono<DeleteRequest> request) {
        return request.map(req -> {
            try {
                dao.delete(req.getKey());
                return DeleteResponse.newBuilder().setSuccess(true).build();
            } catch (Exception e) {
                return DeleteResponse.newBuilder().setSuccess(false).build();
            }
        });
    }
}
