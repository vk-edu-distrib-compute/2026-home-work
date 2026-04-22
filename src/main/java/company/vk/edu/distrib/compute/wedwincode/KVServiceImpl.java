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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class KVServiceImpl implements KVService {
    public static final String GET_METHOD = "GET";
    public static final String PUT_METHOD = "PUT";
    public static final String DELETE_METHOD = "DELETE";
    private static final int THREADS = 4;
    private static final int EMPTY_RESPONSE = -1;

    private boolean isStarted;
    private final Dao<DaoRecord> dao;
    protected final HttpServer server;
    private final ExecutorService executor;

    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public KVServiceImpl(int port, Dao<DaoRecord> dao) throws IOException {
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
        closeDao();
    }

    protected void closeDao() {
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
        Map<String, String> params;
        try {
            params = parseQuery(exchange.getRequestURI().getRawQuery());
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }

        handleEntityMethod(params, exchange);
    }

    protected void handleEntityMethod(Map<String, String> params, HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case GET_METHOD -> handleGetEntity(params, exchange);
                case PUT_METHOD -> handlePutEntity(params, exchange);
                case DELETE_METHOD -> handleDeleteEntity(params, exchange);
                default -> handleUnsupportedMethod(exchange);
            }
        } catch (IOException | RuntimeException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, exchange);
        }
    }

    private void handleGetEntity(Map<String, String> params, HttpExchange exchange) throws IOException {
        try {
            DaoRecord daoRecord = getValue(params);
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, daoRecord.data().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(daoRecord.data());
                }
            }
        } catch (NoSuchElementException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_NOT_FOUND, exchange);
        }
    }

    private void handlePutEntity(Map<String, String> params, HttpExchange exchange) throws IOException {
        try {
            byte[] body;
            try (InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }

            putValue(params, body);
            sendEmptyResponse(HttpURLConnection.HTTP_CREATED, exchange);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
        }
    }

    private void handleDeleteEntity(Map<String, String> params, HttpExchange exchange) throws IOException {
        try {
            deleteValue(params);
            sendEmptyResponse(HttpURLConnection.HTTP_ACCEPTED, exchange);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
        }
    }

    protected void handleUnsupportedMethod(HttpExchange exchange) throws IOException {
        sendEmptyResponse(HttpURLConnection.HTTP_BAD_METHOD, exchange);
    }

    protected DaoRecord getValue(Map<String, String> params) throws IOException {
        String id = getValueFromParams("id", params);
        return dao.get(id);
    }

    protected void putValue(Map<String, String> params, byte[] data) throws IOException {
        String id = getValueFromParams("id", params);
        dao.upsert(id, DaoRecord.buildCreated(data));
    }

    protected void deleteValue(Map<String, String> params) throws IOException {
        String id = getValueFromParams("id", params);
        dao.delete(id);
    }

    protected static Map<String, String> parseQuery(String query) {
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException();
        }

        Map<String, String> params = new ConcurrentHashMap<>();
        int paramPartsCount = 2;
        for (String q : query.split("&")) {
            String[] split = q.split("=", 2);
            if (split.length != paramPartsCount) {
                continue;
            }

            String key = URLDecoder.decode(split[0], StandardCharsets.UTF_8);
            String value = URLDecoder.decode(split[1], StandardCharsets.UTF_8);

            if (value.isEmpty()) {
                throw new IllegalArgumentException("value is empty");
            }
            params.put(key, value);
        }

        return params;
    }

    protected static String getValueFromParams(String param, Map<String, String> params) {
        String value = params.get(param);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(param + " not provided");
        }
        return value;
    }

    public static void sendEmptyResponse(int code, HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(code, EMPTY_RESPONSE);
        }
    }
}
