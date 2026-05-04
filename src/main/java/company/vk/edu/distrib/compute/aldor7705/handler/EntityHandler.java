package company.vk.edu.distrib.compute.aldor7705.handler;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.aldor7705.EntityDao;
import company.vk.edu.distrib.compute.aldor7705.exceptions.MethodNotAllowedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.NoSuchElementException;

public class EntityHandler extends BaseHttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private final List<Integer> clusterPorts;
    private final int myPort;
    private final EntityDao dao;

    public EntityHandler(EntityDao dao, int myPort, List<Integer> clusterPorts) {
        super();
        this.dao = dao;
        this.myPort = myPort;
        this.clusterPorts = clusterPorts;
    }

    @Override
    protected void handleRequest(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        String id = getIdFromQuery(query);
        int ack = getAckFromQuery(query);

        if (ack > dao.getReplicaCount()) {
            throw new IllegalArgumentException("Переданное значение ack превышает количесвто реплик");
        }

        byte[] requestBody = null;
        if ("PUT".equals(method)) {
            requestBody = exchange.getRequestBody().readAllBytes();
        }

        if (clusterPorts != null && !clusterPorts.isEmpty()) {
            int targetPort = getTargetPort(id);

            if (myPort != targetPort) {
                proxyToNode(exchange, targetPort, id, ack, method, requestBody);
                return;
            }
        }

        switch (method) {
            case "GET":
                byte[] data = getEntityDao(id, ack);
                sendAnswer(exchange, data, 200);
                break;
            case "PUT":
                dao.upsert(id, requestBody, ack);
                sendEmptyAnswer(exchange, 201);
                break;
            case "DELETE":
                dao.delete(id, ack);
                sendEmptyAnswer(exchange, 202);
                break;
            default:
                throw new MethodNotAllowedException("Метод " + method + " не поддерживается для /entity");
        }
    }

    private int getTargetPort(String key) {
        int index = Math.abs(key.hashCode()) % clusterPorts.size();
        return clusterPorts.get(index);
    }

    private void proxyToNode(HttpExchange exchange, int targetPort,
                             String id, int ack, String method, byte[] requestBody) throws IOException {
        String targetUrl = "http://localhost:" + targetPort + "/v0/entity?id=" + id + "&ack=" + ack;

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .method(method,
                        requestBody != null
                                ? HttpRequest.BodyPublishers.ofByteArray(requestBody)
                                : HttpRequest.BodyPublishers.noBody());

        try {
            HttpResponse<byte[]> response = httpClient.send(
                    requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            exchange.getResponseHeaders().putAll(response.headers().map());
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);

            try (var outputStream = exchange.getResponseBody()) {
                outputStream.write(response.body());
            }
        } catch (Exception e) {
            log.error("Ошибка кластера", e);
            sendError(exchange, 500, "Ошибка кластера: " + e.getMessage());
        }
    }

    private String getIdFromQuery(String query) {
        if (query == null) {
            throw new IllegalArgumentException("id отсутствует");
        }
        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith("id=")) {
                String id = param.substring(3);
                if (id.isEmpty()) {
                    throw new IllegalArgumentException("id пуст");
                }
                return id;
            }
        }
        throw new IllegalArgumentException("id отсутствует");
    }

    private int getAckFromQuery(String query) {
        if (query == null) {
            return 1;
        }
        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith("ack=")) {
                String ackValue = param.substring(4);
                if (ackValue.isEmpty()) {
                    return 1;
                }
                return Integer.parseInt(ackValue);
            }
        }
        return 1;
    }

    private byte[] getEntityDao(String id, int ack) throws IOException {
        try {
            return dao.get(id, ack);
        } catch (NoSuchElementException e) {
            log.warn("Ключ {} не найден", id);
            throw new NoSuchElementException("Ключ не найден: " + id, e);
        }
    }
}
