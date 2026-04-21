package company.vk.edu.distrib.compute.nihuaway00.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.replication.InsufficientReplicasException;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {
    private final ShardRouter shardRouter;
    private final ReplicaManager replicaManager;

    public EntityHandler(ReplicaManager replicaManager, ShardRouter shardRouter) {
        this.replicaManager = replicaManager;
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

        int ack;
        try {
            ack = Integer.parseInt(params.get("ack"));
        } catch (NumberFormatException e) {
            ack = 1;
        }

        try (exchange) {
            try {
                String targetNodeEndpoint = shardRouter.getResponsibleNode(id);

                if (!shardRouter.isLocalNode(targetNodeEndpoint)) {
                    try {
                        shardRouter.proxyRequest(exchange, targetNodeEndpoint);
                    } catch (InterruptedException e) {
                        //восстанавливаем флаг isInterrupted, иначе будут проблемы с graceful shutdown
                        Thread.currentThread().interrupt();
                        HttpUtils.sendError(exchange, 503, "Request interrupted");
                        return;
                    }
                    return;
                }

                switch (method) {
                    case "GET" -> {
                        handleGetEntity(exchange, id, ack);
                    }
                    case "PUT" -> {
                        handlePutEntity(exchange, id, ack);
                    }
                    case "DELETE" -> {
                        handleDeleteEntity(exchange, id, ack);
                    }
                    default -> HttpUtils.sendError(exchange, 405, "Method not allowed");
                }
            } catch (NoSuchElementException err) {
                HttpUtils.sendError(exchange, 404, err.getMessage());
            } catch (IllegalArgumentException err) {
                HttpUtils.sendError(exchange, 400, err.getMessage());
            } catch (InsufficientReplicasException err) {
                HttpUtils.sendError(exchange, 500, err.getMessage());
            } catch (Exception err) {
                HttpUtils.sendError(exchange, 503, err.getMessage());
            }
        }
    }

    public void handleGetEntity(HttpExchange exchange, String id, int ack)
            throws IOException, NoSuchElementException, IllegalArgumentException {
        byte[] data = replicaManager.get(id, ack);

        if(data == null){
            throw new NoSuchElementException();
        }

        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    public void handlePutEntity(HttpExchange exchange, String id, int ack)
            throws IOException, IllegalArgumentException {

        try (InputStream is = exchange.getRequestBody()) {
            var data = is.readAllBytes();
            replicaManager.put(id, data, ack);
            exchange.sendResponseHeaders(201, -1);
        }
    }

    public void handleDeleteEntity(HttpExchange exchange, String id, int ack)
            throws IOException, IllegalArgumentException {
        replicaManager.delete(id, ack);
        exchange.sendResponseHeaders(202, -1);
    }
}
