package company.vk.edu.distrib.compute.bobridze5.handlers;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.bobridze5.utils.URLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.NoSuchElementException;

public class EntityHandler extends DaoHandler<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);

    public EntityHandler(Dao<byte[]> dao) {
        super(dao);
    }

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        String id = getEntityId(exchange);
        if (id == null) {
            return;
        }

        try {
            byte[] data = dao.get(id);
            log.info("GET entity with id: {}", id);
            sendResponse(exchange, HttpURLConnection.HTTP_OK, data);
        } catch (NoSuchElementException e) {
            log.warn("Entity not found {}", id);
            sendError(exchange, HttpURLConnection.HTTP_NOT_FOUND);
        } catch (IllegalArgumentException e) {
            log.error("Invalid ID format: {}", id, e);
            sendError(exchange, HttpURLConnection.HTTP_BAD_REQUEST);
        } catch (Exception e) {
            log.error("Failed to GET entity: {}", id, e);
            sendError(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR);
        }
    }

    @Override
    protected void handlePut(HttpExchange exchange) throws IOException {
        String id = getEntityId(exchange);
        if (id == null) {
            return;
        }

        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, body);
            log.info("PUT entity with id: {}, size: {}", id, body.length);
            sendResponse(exchange, HttpURLConnection.HTTP_CREATED, new byte[0]);
        } catch (Exception e) {
            log.error("Failed to PUT entity: {}", id, e);
            sendError(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR);
        }
    }

    @Override
    protected void handleDelete(HttpExchange exchange) throws IOException {
        String id = getEntityId(exchange);
        if (id == null) {
            return;
        }

        try {
            dao.delete(id);
            log.info("DELETE entity with id: {}", id);
            sendResponse(exchange, HttpURLConnection.HTTP_ACCEPTED, new byte[0]);
        } catch (Exception e) {
            log.error("Failed to DELETE entity: {}", id, e);
            sendError(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR);
        }
    }

    private String getEntityId(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String id = URLUtils.parseQueryParams(query).get("id");

        if (id == null || id.isBlank()) {
            log.warn("Request missing required 'id' parameter");
            sendError(exchange, HttpURLConnection.HTTP_BAD_REQUEST);
            return null;
        }

        return id;
    }
}
