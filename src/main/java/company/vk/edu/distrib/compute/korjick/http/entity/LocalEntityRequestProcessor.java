package company.vk.edu.distrib.compute.korjick.http.entity;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.korjick.http.Constants;

import java.io.IOException;
import java.util.NoSuchElementException;

public class LocalEntityRequestProcessor implements EntityRequestProcessor {
    protected final Dao<byte[]> dao;

    public LocalEntityRequestProcessor(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public EntityResponse process(EntityRequest request) throws IOException {
        return switch (request.method()) {
            case Constants.HTTP_METHOD_GET -> {
                byte[] value = dao.get(request.id());
                if (value == null) {
                    throw new NoSuchElementException("No value for key: " + request.id());
                }
                yield new EntityResponse(Constants.HTTP_STATUS_OK, value);
            }
            case Constants.HTTP_METHOD_PUT -> {
                dao.upsert(request.id(), request.body());
                yield new EntityResponse(Constants.HTTP_STATUS_CREATED, null);
            }
            case Constants.HTTP_METHOD_DELETE -> {
                dao.delete(request.id());
                yield new EntityResponse(Constants.HTTP_STATUS_ACCEPTED, null);
            }
            default -> new EntityResponse(Constants.HTTP_STATUS_METHOD_NOT_ALLOWED, null);
        };
    }
}
