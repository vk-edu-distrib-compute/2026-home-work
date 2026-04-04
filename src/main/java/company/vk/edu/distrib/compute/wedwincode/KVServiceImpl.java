package company.vk.edu.distrib.compute.wedwincode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStartException;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStopException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class KVServiceImpl implements KVService {
    private static final int EMPTY_RESPONSE = -1;
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final int THREADS = 4;

    private boolean isStarted;
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final ExecutorService executor;

    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), port), 0);
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
        this.executor = Executors.newFixedThreadPool(THREADS);
        this.server.setExecutor(executor);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (isStarted) {
                throw new ServiceStartException("service already started");
            }
            isStarted = true;
        } finally {
            lifecycleLock.unlock();
        }
        server.start();
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            if (!isStarted) {
                throw new ServiceStopException("service was already stopped");
            }
            isStarted = false;
        } finally {
            lifecycleLock.unlock();
        }

        server.stop(0);
        executor.shutdown();
        try {
            dao.close();
        } catch (IOException e) {
            throw new ServiceStopException("dao was not closed successfully", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            handleUnsupportedMethod(exchange);
            return;
        }
        sendEmptyResponse(HttpURLConnection.HTTP_OK, exchange);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id;
        try {
            id = parseQuery(exchange.getRequestURI().getRawQuery());
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }
        try {
            switch (exchange.getRequestMethod()) {
                case GET_METHOD -> handleGetEntity(id, exchange);
                case PUT_METHOD -> handlePutEntity(id, exchange);
                case DELETE_METHOD -> handleDeleteEntity(id, exchange);
                default -> handleUnsupportedMethod(exchange);
            }
        } catch (IOException | RuntimeException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, exchange);
        }
    }

    private void handleGetEntity(String id, HttpExchange exchange) throws IOException {
        try {
            byte[] data = dao.get(id);
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
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

        String idParamName = "id";
        int paramPartsCount = 2;
        for (String q : query.split("&")) {
            String[] split = q.split("=", 2);
            if (split.length != paramPartsCount) {
                continue;
            }

            String name = URLDecoder.decode(split[0], StandardCharsets.UTF_8);
            String value = URLDecoder.decode(split[1], StandardCharsets.UTF_8);

            if (idParamName.equals(name) && !value.isEmpty()) {
                return value;
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
