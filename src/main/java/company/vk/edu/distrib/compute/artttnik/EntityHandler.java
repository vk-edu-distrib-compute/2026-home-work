package company.vk.edu.distrib.compute.artttnik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

final class EntityHandler implements HttpHandler {
    private final MyReplicatedKVService service;

    EntityHandler(MyReplicatedKVService service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        Map<String, String> params = MyReplicatedKVService.parseQueryParams(exchange.getRequestURI().getRawQuery());
        String id = params.get("id");

        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }
        processEntityRequest(exchange, params, id);
    }

    private void processEntityRequest(HttpExchange exchange, Map<String, String> params, String id)
            throws IOException {
        try {
            int ack = service.parseAck(params.get("ack"));

            if (ack > service.numberOfReplicas()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            byte[] requestBody = "PUT".equals(exchange.getRequestMethod())
                    ? exchange.getRequestBody().readAllBytes()
                    : new byte[0];

            ProxyResult result;
            String internalHeader = exchange.getRequestHeaders().getFirst("X-Internal-Shard-Request");
            if (service.proxyService().shouldProxy(internalHeader, id)) {
                result = service.proxyService().proxy(exchange.getRequestMethod(), id, params.get("ack"), requestBody);
            } else {
                result = service.handleLocalRequest(exchange.getRequestMethod(), id, ack, requestBody);
            }

            writeHttpResponse(exchange, result);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(400, -1);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private void writeHttpResponse(HttpExchange exchange, ProxyResult result) throws IOException {
        if (result.body().length == 0) {
            exchange.sendResponseHeaders(result.statusCode(), -1);
            return;
        }

        exchange.sendResponseHeaders(result.statusCode(), result.body().length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(result.body());
        }
    }
}
