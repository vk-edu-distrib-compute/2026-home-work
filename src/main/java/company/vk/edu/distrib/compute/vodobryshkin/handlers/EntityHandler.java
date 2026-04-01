package company.vk.edu.distrib.compute.vodobryshkin.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vodobryshkin.HttpMethod;
import company.vk.edu.distrib.compute.vodobryshkin.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class EntityHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger("server");

    private final Dao<byte[]> storage;

    public EntityHandler(Dao<byte[]> storage) {
        this.storage = storage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String queryString = exchange.getRequestURI().getQuery();

        log.info("Received a {}-request on /v0/entity?{} endpoint", method, queryString);

        if (!method.equals(HttpMethod.GET.name()) && !method.equals(HttpMethod.PUT.name())
                && !method.equals(HttpMethod.DELETE.name())) {
            exchange.sendResponseHeaders(StatusCode.METHOD_NOT_ALLOWED.getCode(), -1);
            return;
        }

        if (queryString == null || queryString.isBlank()) {
            exchange.sendResponseHeaders(StatusCode.BAD_REQUEST.getCode(), -1);
            return;
        }

        String[] partsOfQueryString = queryString.split("=");

        if (partsOfQueryString.length != 2) {
            exchange.sendResponseHeaders(StatusCode.BAD_REQUEST.getCode(), -1);
            return;
        }

        if ("\"\"".equals(partsOfQueryString[1]) || partsOfQueryString[1].isEmpty()) {
            exchange.sendResponseHeaders(StatusCode.BAD_REQUEST.getCode(), -1);
            return;
        }

        if (!"id".equals(partsOfQueryString[0])) {
            exchange.sendResponseHeaders(StatusCode.UNPROCESSABLE_CONTENT.getCode(), -1);
            return;
        }

        String id = partsOfQueryString[1];
        byte[] result;

        if (method.equals(HttpMethod.GET.name())) {
            try {
                result = storage.get(id);
            } catch (Exception e) {
                exchange.sendResponseHeaders(StatusCode.NOT_FOUND.getCode(), -1);
                return;
            }

            exchange.sendResponseHeaders(StatusCode.OK.getCode(), result.length);

            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(result);
            outputStream.close();

            log.debug("Successfully handled GET-request on /v0/entity?{} endpoint with a result: {}", method, result);
        } else if (method.equals(HttpMethod.DELETE.name())) {
            storage.delete(id);
            exchange.sendResponseHeaders(StatusCode.ACCEPTED.getCode(), -1);

            log.debug("Successfully handled DELETE-request on /v0/entity?{}.", queryString);
        } else {
            byte[] body = exchange.getRequestBody().readAllBytes();

            storage.upsert(id, body);
            exchange.sendResponseHeaders(StatusCode.CREATED.getCode(), -1);

            log.debug("Successfully handled PUT-request on /v0/entity?{}.", queryString);
        }
    }
}
