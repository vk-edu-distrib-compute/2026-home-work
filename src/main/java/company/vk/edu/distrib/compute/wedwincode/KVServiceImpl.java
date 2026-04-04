package company.vk.edu.distrib.compute.wedwincode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServerStopException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private static final int EMPTY_RESPONSE = -1;

    private boolean isStarted;
    private final Dao<byte[]> dao;
    private final HttpServer server;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
        this.server.setExecutor(Executors.newFixedThreadPool(4));
    }

    @Override
    public synchronized void start() {
        if (isStarted) {
            return;
        }
        isStarted = true;
        server.start();
    }

    @Override
    public synchronized void stop() {
        if (!isStarted) {
            return;
        }
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            throw new ServerStopException("dao was not closed successfully", e);
        }
        isStarted = false;
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        sendEmptyResponse(HttpURLConnection.HTTP_OK, exchange);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id;
        try {
            id = parseQuery(exchange.getRequestURI().getQuery());
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGetEntity(id, exchange);
            case "PUT" -> handlePutEntity(id, exchange);
            case "DELETE" -> handleDeleteEntity(id, exchange);
            default -> handleUnsupportedMethod(exchange);
        }
    }

    private void handleGetEntity(String id, HttpExchange exchange) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        } catch (NoSuchElementException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_NOT_FOUND, exchange);
        }
    }

    private void handlePutEntity(String id, HttpExchange exchange) throws IOException {
        try {
            byte[] body;
            try (InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }

            dao.upsert(id, body);
            sendEmptyResponse(HttpURLConnection.HTTP_CREATED, exchange);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
        }
    }

    private void handleDeleteEntity(String id, HttpExchange exchange) throws IOException {
        try {
            dao.delete(id);
            sendEmptyResponse(HttpURLConnection.HTTP_ACCEPTED, exchange);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
        }
    }

    private void handleUnsupportedMethod(HttpExchange exchange) throws IOException {
        sendEmptyResponse(HttpURLConnection.HTTP_BAD_METHOD, exchange);
    }

    private static String parseQuery(String query) {
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException();
        }

        for (String q : query.split("&")) {
            String[] split = q.split("=", 2);
            if (split.length == 2 && "id".equals(split[0]) && !split[1].isEmpty()) {
                return split[1];
            }
        }

        throw new IllegalArgumentException();
    }

    private static void sendEmptyResponse(int code, HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(code, EMPTY_RESPONSE);
        }
    }
}
