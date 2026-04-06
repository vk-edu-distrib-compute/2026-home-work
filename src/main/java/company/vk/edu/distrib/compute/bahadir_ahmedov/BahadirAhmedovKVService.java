package company.vk.edu.distrib.compute.bahadir_ahmedov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;

public class BahadirAhmedovKVService implements KVService {
    private final Dao<byte[]> storage;
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    private static final Logger log = LoggerFactory.getLogger(BahadirAhmedovKVService.class);

    private final HttpServer server;

    public BahadirAhmedovKVService(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        storage = new InMemoryDao();

        initServer();
    }

    private void initServer() {
            server.createContext("/v0/status", this::handleStatus);
            server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
        log.info("Started");
    }

    @Override
    public void stop() {
        log.info("Stopping");
        server.stop(0);
        log.info("Stopped");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, GET)) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {

        String id = getId(exchange);

        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, 0);
            exchange.close();
            return;
        }

        switch (exchange.getRequestMethod()) {
            case GET -> handleGet(exchange, id);
            case PUT -> handlePut(exchange, id);
            case DELETE -> handleDelete(exchange, id);
            default -> {
                exchange.sendResponseHeaders(405, 0);
                exchange.close();
            }
        }
    }

    private String getId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        }
        return null;
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        storage.upsert(id, body);
        sendResponse(exchange, 201, "Created".getBytes());
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = storage.get(id);
        if (value == null) {
            sendResponse(exchange, 404, "Not Found".getBytes());
            return;
        }
        sendResponse(exchange, 200, value);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        storage.delete(id);
        sendResponse(exchange, 202, "Accepted".getBytes());
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] data) throws IOException {
        exchange.sendResponseHeaders(code, data.length);
        exchange.getResponseBody().write(data);
        exchange.close();
    }
}
