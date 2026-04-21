package company.vk.edu.distrib.compute.proteusp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.proteusp.dao.ProteusPFSDao;
//import company.vk.edu.distrib.compute.proteusp.dao.ProteusPInMemoryDao;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class ProteuspKVService implements KVService {

    protected static final String METHOD_GET = "GET";
    protected static final String METHOD_PUT = "PUT";
    protected static final String METHOD_DELETE = "DELETE";
    protected static final String PARAM_ID = "id";

    protected final ReentrantLock lock = new ReentrantLock();

    protected final ExecutorService executor;
    protected HttpServer server;
    protected boolean running;
    protected Dao<byte[]> dao;
    protected Integer port;

    public ProteuspKVService(int port, Dao<byte[]> dao) {
        this.executor = Executors.newCachedThreadPool();
        this.dao = dao;
        this.port = port;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create HTTP server on port " + port, e);
        }

        server.setExecutor(executor);
        server.createContext("/v0/status", handleErrors(this::handleStatus));
        server.createContext("/v0/entity", handleErrors(this::handleEntity));

    }

    public ProteuspKVService(int port) {
        this(port, new ProteusPFSDao(KVService.class));
    }

    static HttpHandler handleErrors(HttpHandler handler) {
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

    void handleStatus(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        if (!METHOD_GET.equals(method)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    protected void handleEntity(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params = HttpUtils.parseQueryParams(query);

        switch (method) {
            case METHOD_GET -> handleGetEntity(exchange, params);
            case METHOD_PUT -> handlePutEntity(exchange, params);
            case METHOD_DELETE -> handleDeleteEntity(exchange, params);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleDeleteEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get(PARAM_ID);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        dao.delete(key);
        exchange.sendResponseHeaders(202, -1);
    }

    private void handleGetEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get(PARAM_ID);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        try {
            byte[] data = dao.get(key);
            HttpUtils.sendResponse(exchange, 200, data);
        } catch (NoSuchElementException e) {
            throw new NoSuchElementException("Key not found: " + key, e);
        }
    }

    private void handlePutEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String key = params.get(PARAM_ID);
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
    public void start() {
        lock.lock();
        try {
            server.start();
            running = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (!running) {
                return;
            }
            running = false;
        } finally {
            lock.unlock();
        }

        server.stop(0);

        try {
            dao.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close DAO", e);
        }
        executor.shutdown();
    }
}
