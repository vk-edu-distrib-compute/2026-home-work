package company.vk.edu.distrib.compute.nihuaway00.transport.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.replication.InsufficientReplicasException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityHttpHandler implements HttpHandler {
    private final KVCommandService commandService;

    public EntityHttpHandler(KVCommandService kvCommandService) {
        commandService = kvCommandService;
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

        int ack = parseAck(params);

        try (exchange) {
            try {
                dispatchByMethod(exchange, method, id, ack);
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

    private int parseAck(Map<String, String> params) {
        String ackParam = params.get("ack");
        if (ackParam == null) {
            return commandService.replicaManager.numberOfReplicas();
        }
        return Integer.parseInt(ackParam);
    }

    private void dispatchByMethod(HttpExchange exchange, String method, String id, int ack) throws IOException {
        switch (method) {
            case "GET" -> {
                byte[] data = commandService.handleGetEntity(id, ack);
                exchange.sendResponseHeaders(200, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
            }
            case "PUT" -> {
                try (InputStream is = exchange.getRequestBody()) {
                    byte[] data = is.readAllBytes();
                    commandService.handlePutEntity(id, data, ack);
                    exchange.sendResponseHeaders(201, -1);
                }
            }
            case "DELETE" -> {
                commandService.handleDeleteEntity(id, ack);
                exchange.sendResponseHeaders(202, -1);
                exchange.close();
            }
            default -> HttpUtils.sendError(exchange, 405, "Method not allowed");
        }
    }
}
