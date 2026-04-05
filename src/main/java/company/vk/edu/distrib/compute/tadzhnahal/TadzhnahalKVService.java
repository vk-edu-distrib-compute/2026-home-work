package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class TadzhnahalKVService implements KVService {
    private static final String statusPath = "/v0/status";
    private static final String entityPath = "/v0/entity";

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;

    public TadzhnahalKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (server != null) {
            throw new IllegalStateException("Server already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(statusPath, this::handleStatus);
            server.createContext(entityPath, this::handleEntity);
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void stop() {
        if (server == null) {
            throw new IllegalStateException("Server is not started");
        }

        server.stop(0);
        server = null;

        try {
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot close dao", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!statusPath.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, 404);
                return;
            }

            if (!"GET".equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, 405);
                return;
            }

            sendEmptyResponse(exchange, 200);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                if (!entityPath.equals(exchange.getRequestURI().getPath())) {
                    sendEmptyResponse(exchange, 404);
                    return;
                }

                String id = extractId(exchange);

                switch (exchange.getRequestMethod()) {
                    case "GET":
                        handleGetEntity(exchange, id);
                        break;
                    case "PUT":
                        handlePutEntity(exchange, id);
                        break;
                    case "DELETE":
                        handleDeleteEntity(exchange, id);
                        break;
                    default:
                        sendEmptyResponse(exchange, 405);
                        break;
                }
            } catch (IllegalArgumentException e) {
                sendEmptyResponse(exchange, 400);
            } catch (NoSuchElementException e) {
                sendEmptyResponse(exchange, 404);
            } catch (IOException e) {
                sendEmptyResponse(exchange, 500);
            }
        }
    }

    private void handleGetEntity(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        exchange.getResponseBody().write(value);
    }

    private void handlePutEntity(HttpExchange exchange, String id) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);
        sendEmptyResponse(exchange, 201);
    }

    private void handleDeleteEntity(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendEmptyResponse(exchange, 202);
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();

        if (query == null) {
            throw new IllegalArgumentException("Missing query");
        }

        if (!query.startsWith("id=")) {
            throw new IllegalArgumentException("Missing id parameter");
        }

        if (query.contains("&")) {
            throw new IllegalArgumentException("Unexpected parameters");
        }

        String id = query.substring("id=".length());

        if (id.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }

        return id;
    }

    private void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }
}
