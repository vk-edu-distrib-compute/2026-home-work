package company.vk.edu.distrib.compute.handlest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.handlest.enums.HandlestHttpStatus;
import company.vk.edu.distrib.compute.handlest.exceptions.PortRangeException;
import company.vk.edu.distrib.compute.handlest.exceptions.ServerStopException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HandlestService implements KVService {
    private final HttpServer server;
    private final HandlestDao dao;
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    private static final int MIN_PORT_VALUE = 1024; // values less than 1024 are reserved by system
    private static final int MAX_PORT_VALUE = 65535;
    private static final String PATH_TO_LOCAL_STORAGE_FOLDER = "handlest_storage";
    private static final String ID_PARAM = "id";

    public HandlestService(int port) throws IOException {
        if (port < MIN_PORT_VALUE || port > MAX_PORT_VALUE) {
            throw new PortRangeException(
                    "Port must be between " + MIN_PORT_VALUE + " and " + MAX_PORT_VALUE + ", got: " + port);
        }

        this.dao = new HandlestDao(PATH_TO_LOCAL_STORAGE_FOLDER);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
        server.setExecutor(Executors.newFixedThreadPool(10));
    }

    @Override
    public void start() {
        server.start();
        healthy.set(true);
    }

    @Override
    public void stop() {
        healthy.set(false);
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            throw new ServerStopException("Unexpected error occurred. Could not shutdown server", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("GET".equals(exchange.getRequestMethod())) {
                int statusCode = healthy.get()
                        ? HandlestHttpStatus.OK.getCode()
                        : HandlestHttpStatus.SERVICE_UNAVAILABLE.getCode();
                exchange.sendResponseHeaders(statusCode, -1);
            } else {
                exchange.sendResponseHeaders(HandlestHttpStatus.SERVICE_UNAVAILABLE.getCode(), -1);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
                return;
            }

            switch (method) {
                case "GET":
                    handleGet(exchange, id);
                    break;
                case "PUT":
                    handlePut(exchange, id);
                    break;
                case "DELETE":
                    handleDelete(exchange, id);
                    break;
                default:
                    exchange.sendResponseHeaders(HandlestHttpStatus.METHOD_NOT_ALLOWED.getCode(), -1);
                    break;
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.INTERNAL_ERROR.getCode(), -1);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(HandlestHttpStatus.OK.getCode(), data.length);
            exchange.getResponseBody().write(data);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.NOT_FOUND.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(HandlestHttpStatus.CREATED.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            exchange.sendResponseHeaders(HandlestHttpStatus.ACCEPTED.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private String extractId(String query) {
        if (query == null) {
            return null;
        }

        String[] params = query.split("&");
        for (String param : params) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2 && ID_PARAM.equals(keyValue[0])) {
                return keyValue[1];
            }
        }

        return null;
    }
}
