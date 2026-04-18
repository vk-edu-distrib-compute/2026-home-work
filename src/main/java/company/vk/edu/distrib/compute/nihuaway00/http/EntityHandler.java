package company.vk.edu.distrib.compute.nihuaway00.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.sharding.NodeInfo;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {
    private final ShardRouter shardRouter;
    private final EntityDao dao;

    public EntityHandler(EntityDao dao, ShardRouter shardRouter) {
        this.dao = dao;
        this.shardRouter = shardRouter;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        Map<String, String> params = HttpUtils.parseQuery(uri.getQuery());
        String id = params.get("id");

        if (id == null || id.isBlank()) {
            HttpUtils.sendError(exchange, 400, "id is required");
            return;
        }

        try (exchange) {
            try {
                NodeInfo targetNode = shardRouter.getNodeEndpoint(id);

                if (!shardRouter.isLocalNode(targetNode.getEndpoint())) {
                    try {
                        shardRouter.proxyRequest(exchange, targetNode.getEndpoint());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); //восстанавливаем флаг isInterrupted, иначе будут проблемы с graceful shutdown
                        throw new RuntimeException(e);
                    }
                    return;
                }
                switch (method) {
                    case "GET" -> {
                        handleGetEntity(exchange, id);
                    }
                    case "PUT" -> {
                        handlePutEntity(exchange, id);
                    }
                    case "DELETE" -> {
                        handleDeleteEntity(exchange, id);
                    }
                    default -> HttpUtils.sendError(exchange, 405, "Method not allowed");
                }
            } catch (NoSuchElementException err) {
                HttpUtils.sendError(exchange, 404, err.getMessage());
            } catch (IllegalArgumentException err) {
                HttpUtils.sendError(exchange, 400, err.getMessage());
            } catch (Exception err) {
                HttpUtils.sendError(exchange, 503, err.getMessage());
            }
        }
    }

    public void handleGetEntity(HttpExchange exchange, String id)
            throws IOException, NoSuchElementException, IllegalArgumentException {
        byte[] data = dao.get(id);
        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    public void handlePutEntity(HttpExchange exchange, String id)
            throws IOException, IllegalArgumentException {

        try (InputStream is = exchange.getRequestBody()) {
            var data = is.readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(201, -1);
        }
    }

    public void handleDeleteEntity(HttpExchange exchange, String id)
            throws IOException, IllegalArgumentException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }
}
