package company.vk.edu.distrib.compute.vitos23;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vitos23.util.HttpCodes;

import java.io.IOException;
import java.util.Objects;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.NO_BODY_RESPONSE_LENGTH;

public class DirectEntityRequestProcessor implements EntityRequestProcessor {
    private final Dao<byte[]> dao;

    public DirectEntityRequestProcessor(Dao<byte[]> dao) {
        this.dao = Objects.requireNonNull(dao, "Dao must not be null");
    }

    @Override
    public void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(HttpCodes.OK, value.length);
        exchange.getResponseBody().write(value);
    }

    @Override
    public void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(HttpCodes.CREATED, NO_BODY_RESPONSE_LENGTH);
    }

    @Override
    public void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(HttpCodes.ACCEPTED, NO_BODY_RESPONSE_LENGTH);
    }
}
