package company.vk.edu.distrib.compute.denchika.service;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InMemoryKVService implements KVService {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PARAM_ID = "id";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final Logger log = LoggerFactory.getLogger(InMemoryKVService.class);

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private ExecutorService executor;

    public InMemoryKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext(PATH_STATUS, this::handleStatus);
            server.createContext(PATH_ENTITY, this::handleEntity);

            executor = Executors.newFixedThreadPool(4);
            server.setExecutor(executor);
            server.start();

        } catch (IOException e) {
            log.error("Failed to start server", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdown();
        }

        try {
            dao.close();
        } catch (IOException e) {
            log.error("Failed to close DAO", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        exchange.sendResponseHeaders(200, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = extractId(exchange.getRequestURI());
        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method)) {
            doGet(exchange, id);
        } else if (METHOD_PUT.equals(method)) {
            doPut(exchange, id);
        } else if (METHOD_DELETE.equals(method)) {
            doDelete(exchange, id);
        } else {
            exchange.sendResponseHeaders(405, -1);
        }
    }

    private void doGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);

            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, value.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }

        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void doPut(HttpExchange exchange, String id) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] body = is.readAllBytes();
            dao.upsert(id, body);
        }
        exchange.sendResponseHeaders(201, -1);
    }

    private void doDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }

    private static String extractId(URI uri) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }

        for (String p : query.split("&")) {
            String[] kv = p.split("=", 2);
            if (kv.length == 2 && PARAM_ID.equals(kv[0])) {
                return kv[1];
            }
        }
        return null;
    }
}
