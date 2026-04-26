package company.vk.edu.distrib.compute.expanse.handler;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.dto.proto.DeleteEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.DeleteEntityResponse;
import company.vk.edu.distrib.compute.expanse.dto.proto.EntityServiceGrpc;
import company.vk.edu.distrib.compute.expanse.dto.proto.GetEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.GetEntityResponse;
import company.vk.edu.distrib.compute.expanse.dto.proto.PutEntityRequest;
import company.vk.edu.distrib.compute.expanse.dto.proto.PutEntityResponse;
import company.vk.edu.distrib.compute.expanse.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.expanse.service.KVStorageService;
import company.vk.edu.distrib.compute.expanse.service.impl.KVStorageServiceImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcHandler extends EntityServiceGrpc.EntityServiceImplBase {
    private final KVStorageService<String, byte[]> kvStorageService;
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcHandler.class);

    public GrpcHandler() {
        super();
        kvStorageService = AppContextUtils.getBean(KVStorageServiceImpl.class);
    }

    @Override
    public void getEntity(GetEntityRequest request, StreamObserver<GetEntityResponse> responseObserver) {
        byte[] entity;
        try {
            entity = kvStorageService.getEntityByID(request.getId());

        } catch (EntityNotFoundException e) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Entity not found: {}", e.getMessage());
            }
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            return;

        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error while getting entity: {}", e.getMessage());
            }
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }
        GetEntityResponse response = GetEntityResponse.newBuilder()
                .setCode(200)
                .setResponse(ByteString.copyFrom(entity))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putEntity(PutEntityRequest request, StreamObserver<PutEntityResponse> responseObserver) {
        try {
            kvStorageService.putEntity(request.getId(), request.getBody().toByteArray());
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error while putting entity: {}", e.getMessage());
            }
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }
        PutEntityResponse response = PutEntityResponse.newBuilder()
                .setCode(201)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteEntity(DeleteEntityRequest request, StreamObserver<DeleteEntityResponse> responseObserver) {
        try {
            kvStorageService.deleteEntityByID(request.getId());

        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error while deleting entity: {}", e.getMessage());
            }
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }
        DeleteEntityResponse response = DeleteEntityResponse.newBuilder()
                .setCode(202)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
