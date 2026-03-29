package company.vk.edu.distrib.compute.ronshinvsevolod;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class MyKVService implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public MyKVService(Dao<byte[]> dao, int port) throws IOException {
        this.dao = dao;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler());
        server.setExecutor(Executors.newCachedThreadPool());
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }

            switch (method) {
                case "GET":
                    handleGet(exchange, id);
                    break;
                case "PUT":
                    handlePut(exchange,id);
                    break;
                case "DELETE":
                    handleDelete(exchange, id);
                    break;
                default:
                    exchange.sendResponseHeaders(405, -1);
                    break;
            }
            exchange.close();
        }
        
        private void handleGet(HttpExchange exchange, String id) throws IOException {
            try {
                byte[] data = dao.get(id);
                exchange.sendResponseHeaders(200, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                exchange.sendResponseHeaders(201, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            try {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private String extractId(String query) {
            if (query == null) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == 2 && "id".equals(pair[0])) {
                    return pair[1];
                }
            }
            return null;
        }
    }
}
