package company.vk.edu.distrib.compute.yyaarr;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;

import java.io.IOException;
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
        private static final String ID_PREFIX = "id=";

        EntityHandler(Dao<byte[]> dao, Logger logger) {
            this.dao = dao;
            this.logger = logger;
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
            }
            switch (requestMethod) {
                case "GET": {
                    try {
                        final var value = dao.get(id);
                        t.sendResponseHeaders(200, 0);
                        t.getResponseBody().write(value);
                    } catch (IllegalArgumentException | NoSuchElementException | IOException exception) {
                        t.sendResponseHeaders(404, 0);
                        t.getResponseBody().write(exception.getMessage().getBytes());
                    }
                    break;
                }
                case "PUT": {
                    try {
                        dao.upsert(id, t.getRequestBody().readAllBytes());
                        t.sendResponseHeaders(201, 0);
                    } catch (IllegalArgumentException | IOException exception) {
                        t.sendResponseHeaders(400, 0);
                        t.getResponseBody().write(exception.getMessage().getBytes());
                    }
                    break;
                }
                case "DELETE": {
                    try {
                        dao.delete(id);
                        t.sendResponseHeaders(202, 0);
                    } catch (IllegalArgumentException | IOException exception) {
                        t.sendResponseHeaders(404, 0);
                        t.getResponseBody().write(exception.getMessage().getBytes());
                    }
                    break;
                }
                default: {
                    t.sendResponseHeaders(503, 0);
                    break;
                }
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




