package company.vk.edu.distrib.compute.proteusp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.proteusp.dao.ProteusPInMemoryDao;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProteuspKVService implements KVService {

    private final ExecutorService executor;
    private final HttpServer server;
    private boolean running;
    private Dao<byte[]> dao;

    public ProteuspKVService(int port, Dao<byte[]> dao) {
        this.executor = Executors.newCachedThreadPool();
        this.dao = dao;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        server.setExecutor(executor);
        server.createContext("/v0/status", handleErrors(this::handleStatus));
        server.createContext("/v0/entity", handleErrors(this::handleEntity));

    }

    public ProteuspKVService(int port) {
        this(port, new ProteusPInMemoryDao());
    }

    private HttpHandler handleErrors(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    HttpUtils.sendError(exchange, 404, e.getMessage());
                } catch (IllegalArgumentException e) {
                    HttpUtils.sendError(exchange, 400, e.getMessage());
                } catch (Exception e) {
                    HttpUtils.sendError(exchange, 503, e.getMessage());
            }
        }
    };
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        if (!"GET".equals(method)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params = HttpUtils.parseQueryParams(query);

        switch (method) {
            case "GET" -> handleGetEntity(exchange, params);
            case "PUT" -> handlePutEntity(exchange, params);
            case "DELETE" -> handleDeleteEntity(exchange, params);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleDeleteEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get("id");
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        dao.delete(key);
        exchange.sendResponseHeaders(202, -1);
    }

    private void handleGetEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get("id");
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        byte[] data = dao.get(key);
        if (data == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        HttpUtils.sendResponse(exchange, 200, data);
    }

    private void handlePutEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get("id");
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        byte[] data;
        try (InputStream is = exchange.getRequestBody()) {
            data = is.readAllBytes();
        }
        dao.upsert(key, data);
        exchange.sendResponseHeaders(201, -1);

    }

    @Override
    public synchronized void start() {
        server.start();
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        server.stop(0);

        try {
            dao.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        executor.shutdown();
    }
}
