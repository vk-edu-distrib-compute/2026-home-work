package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private HttpServer server;
    private final Dao<byte[]> dao;
    private ExecutorService executor;
    private boolean started;

    public KVServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new H2Dao("node" + port);
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        createContext();
        server.start();
        started = true;
    }

    @Override
    public void stop() {
        if (!started) {
            return;
        }
        server.stop(0);
        if (executor != null) {
            executor.close();
        }
        server = null;
        started = false;
    }

    private void createContext() {
        server.createContext("/v0/status", handleStatusRequest());
        server.createContext("/v0/entity", handleEntityRequest());
    }

    private HttpHandler handleStatusRequest() {
        return exchange -> {
            String method = exchange.getRequestMethod();
            if ("GET".equals(method)) {
                exchange.sendResponseHeaders(200, 0);
            } else {
                exchange.sendResponseHeaders(405, 0);
            }
            exchange.close();
        };
    }

    private HttpHandler handleEntityRequest() {
        return exchange -> {
            try (exchange) {
                try {
                    String method = exchange.getRequestMethod();
                    String query = exchange.getRequestURI().getQuery();
                    String id = parseId(query);
                    switch (method) {
                        case "GET" -> {
                            byte[] value = dao.get(id);
                            exchange.sendResponseHeaders(200, value.length);
                            exchange.getResponseBody().write(value);
                        }
                        case "PUT" -> {
                            byte[] newValue = exchange.getRequestBody().readAllBytes();
                            dao.upsert(id, newValue);
                            exchange.sendResponseHeaders(201, 0);
                        }
                        case "DELETE" -> {
                            dao.delete(id);
                            exchange.sendResponseHeaders(202, 0);
                        }
                        default -> exchange.sendResponseHeaders(405, 0);
                    }
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, 0);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, 0);
                }
            }
        };
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }
}
