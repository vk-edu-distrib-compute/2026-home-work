package company.vk.edu.distrib.compute.patersss;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

public class DumpEntityHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpEntityHandler.class);
    public static final String GET_METHOD = "GET";
    public static final String PUT_METHOD = "PUT";
    public static final String DELETE_METHOD = "DELETE";

    private final Dao<byte[]> dao;

    public DumpEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Handling /v0/entity method={}, query={}", method, query);
            }

            final String id;
            try {
                id = UrlParamsUtils.getIdFromQuery(query);
            } catch (IllegalArgumentException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Bad request for /v0/entity: {}", e.getMessage());
                }
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            switch (method) {
                case GET_METHOD -> handleGet(exchange, id);
                case PUT_METHOD -> handlePut(exchange, id);
                case DELETE_METHOD -> handleDelete(exchange, id);
                default -> exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);

            if (value.length == 0) {
                exchange.sendResponseHeaders(200, -1);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("GET /v0/entity success, id={}, bytes=0", id);
                }
                return;
            }

            exchange.sendResponseHeaders(200, value.length);
            exchange.getResponseBody().write(value);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("GET /v0/entity success, id={}, bytes={}", id, value.length);
            }
        } catch (NoSuchElementException e) {
            LOGGER.error("GET /v0/entity not found, id={}", id);
            exchange.sendResponseHeaders(404, -1);
        } catch (IllegalArgumentException e) {
            LOGGER.error("GET /v0/entity bad request, id={}", id, e);
            exchange.sendResponseHeaders(400, -1);
        } catch (IOException e) {
            LOGGER.error("GET /v0/entity failed, id={}", id, e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, body);
            exchange.sendResponseHeaders(201, -1);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("PUT /v0/entity success, id={}, bytes={}", id, body.length);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("PUT /v0/entity bad request, id={}", id, e);
            exchange.sendResponseHeaders(400, -1);
        } catch (IOException e) {
            LOGGER.error("PUT /v0/entity failed, id={}", id, e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            exchange.sendResponseHeaders(202, -1);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("DELETE /v0/entity success, id={}", id);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("DELETE /v0/entity bad request, id={}", id, e);
            exchange.sendResponseHeaders(400, -1);
        } catch (IOException e) {
            LOGGER.error("DELETE /v0/entity failed, id={}", id, e);
            exchange.sendResponseHeaders(500, -1);
        }
    }
}
