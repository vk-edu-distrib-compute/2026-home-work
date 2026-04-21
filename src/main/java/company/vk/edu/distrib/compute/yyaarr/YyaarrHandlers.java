package company.vk.edu.distrib.compute.yyaarr;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.NoSuchElementException;

public class YyaarrHandlers {

    public static class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String requestMethod = t.getRequestMethod();
            if ("GET".equalsIgnoreCase(requestMethod)) {
                t.sendResponseHeaders(200, 0);
            } else {
                t.sendResponseHeaders(503, 0);
            }
            t.close();
        }
    }

    public static class EntityHandler implements HttpHandler {
        private final Dao<byte[]> dao;
        private final Logger logger;
        private final YyaarrKVCluster cluster;
        private final int myPort;
        private final HttpClient httpClient = HttpClient.newHttpClient();

        private static final String ID_PREFIX = "id=";

        EntityHandler(Dao<byte[]> dao, Logger logger) {
            this(dao, logger, null, -1);
        }

        EntityHandler(Dao<byte[]> dao, Logger logger, YyaarrKVCluster cluster, int myPort) {
            this.dao = dao;
            this.logger = logger;
            this.cluster = cluster;
            this.myPort = myPort;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            final String requestMethod = t.getRequestMethod();
            logger.info("requestMethod: {}", requestMethod);
            final String query = t.getRequestURI().getQuery();
            logger.info("query: {}", query);
            final String id = parseId(query);
            logger.info("id: {}", id);
            if (id.isBlank()) {
                t.sendResponseHeaders(400, 0);
                t.getResponseBody().close();
                t.close();
            }

            if (cluster == null) {
                handleLocally(t, requestMethod, id);
                return;
            }

            int targetPort = cluster.getPortForKey(id);

            if (targetPort == myPort) {
                handleLocally(t, requestMethod, id);
            } else {
                proxyRequest(t, targetPort, query, requestMethod);
            }
        }

        private void handleLocally(HttpExchange t, String requestMethod, String id) throws IOException {

            switch (requestMethod) {
                case "GET" -> handleGet(t, id);
                case "PUT" -> handlePut(t, id);
                case "DELETE" -> handleDelete(t, id);
                default -> t.sendResponseHeaders(503, 0);
            }
            t.close();
        }

        private void handleGet(HttpExchange t, String id) throws IOException {
            try {
                final var value = dao.get(id);
                t.sendResponseHeaders(200, value.length);
                t.getResponseBody().write(value);
            } catch (IllegalArgumentException | NoSuchElementException | IOException exception) {
                t.sendResponseHeaders(404, 0);
                t.getResponseBody().write(exception.getMessage().getBytes());
            }
        }

        private void handlePut(HttpExchange t, String id) throws IOException {
            try {
                final var body = t.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                t.sendResponseHeaders(201, 0);
            } catch (IllegalArgumentException | IOException exception) {
                t.sendResponseHeaders(400, 0);
                t.getResponseBody().write(exception.getMessage().getBytes());
            }
        }

        private void handleDelete(HttpExchange t, String id) throws IOException {
            try {
                dao.delete(id);
                t.sendResponseHeaders(202, 0);
            } catch (IllegalArgumentException | IOException exception) {
                t.sendResponseHeaders(404, 0);
                t.getResponseBody().write(exception.getMessage().getBytes());
            }
        }

        private void proxyRequest(HttpExchange t, int targetPort, String query, String method) throws IOException {
            String targetUrl = "http://localhost:" + targetPort + "/v0/entity?" + query;
            logger.info("Proxying to: {}", targetUrl);

            try {
                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(new URI(targetUrl))
                        .timeout(Duration.ofSeconds(5));

                switch (method) {
                    case "GET" -> requestBuilder.GET();
                    case "PUT" -> {
                        byte[] body = t.getRequestBody().readAllBytes();
                        requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                    }
                    case "DELETE" -> requestBuilder.DELETE();
                    default -> {
                        t.sendResponseHeaders(503, 0);
                        return;
                    }
                }

                HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(),
                        HttpResponse.BodyHandlers.ofByteArray());

                t.sendResponseHeaders(response.statusCode(), response.body().length);
                t.getResponseBody().write(response.body());

            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Proxy failed: target node {} not available", targetPort);
                    logger.error("Proxy error: {}", e.getMessage());
                }
                t.sendResponseHeaders(404, 0);
                t.getResponseBody().write(("Node " + targetPort + " not available").getBytes());
            }
            t.close();
        }

        private static String parseId(String query) {
            if (query == null || !query.startsWith(ID_PREFIX)) {
                throw new IllegalArgumentException("Invalid id: " + query);
            } else {
                return query.substring(ID_PREFIX.length());
            }
        }
    }
}




