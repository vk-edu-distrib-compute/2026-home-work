package company.vk.edu.distrib.compute.wedwincode.replicated;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static company.vk.edu.distrib.compute.wedwincode.KVServiceImpl.GET_METHOD;
import static company.vk.edu.distrib.compute.wedwincode.KVServiceImpl.sendEmptyResponse;

public class ReplicationStatsService {
    private final AtomicIntegerArray keysByReplica;
    private final AtomicIntegerArray requestsByReplica;
    private final long startTime;
    private final String statsPrefix;

    public ReplicationStatsService(int replicasCount, String statsPrefix) {
        this.statsPrefix = statsPrefix;
        keysByReplica = new AtomicIntegerArray(replicasCount);
        requestsByReplica = new AtomicIntegerArray(replicasCount);
        startTime = System.currentTimeMillis();
    }

    public void handleStats(HttpExchange exchange) throws IOException {
        String[] parts = getParts(exchange);
        if (parts == null) {
            return;
        }

        Integer id = getId(exchange, parts);
        if (id == null) {
            return;
        }

        final int keyModePartLength = 1;
        final int accessPartLength = 2;
        final String accessPart = "access";

        if (parts.length == keyModePartLength) {
            handleKeyStats(id, exchange);
        } else if (parts.length == accessPartLength && accessPart.equals(parts[1])) {
            handleAccessStats(id, exchange);
        }
    }

    public void incrementKeysCount(int replicaId) {
        keysByReplica.addAndGet(replicaId, 1);
    }

    public void incrementRequestCount(int replicaId) {
        requestsByReplica.addAndGet(replicaId, 1);
    }

    private int getKeysCount(int replicaId) {
        return keysByReplica.get(replicaId);
    }

    private float getAccessRate(int replicaId) {
        long elapsedMillis = System.currentTimeMillis() - startTime;
        if (elapsedMillis <= 0) {
            return 0.0f;
        }
        double elapsedSeconds = elapsedMillis / 1000.0;
        return (float) (requestsByReplica.get(replicaId) / elapsedSeconds);
    }

    private void handleKeyStats(int id, HttpExchange exchange) throws IOException {
        try (exchange) {
            int keysCountRaw = getKeysCount(id);
            String keysCount = String.valueOf(keysCountRaw);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, keysCount.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(keysCount.getBytes());
            }
        }
    }

    private void handleAccessStats(int id, HttpExchange exchange) throws IOException {
        try (exchange) {
            float accessRateRaw = getAccessRate(id);
            String accessRate = String.valueOf(accessRateRaw);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, accessRate.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(accessRate.getBytes());
            }
        }
    }

    private String[] getParts(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_METHOD, exchange);
            return null;
        }

        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(statsPrefix)) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return null;
        }

        String suffix = path.substring(statsPrefix.length());
        if (suffix.isEmpty() || suffix.startsWith("/") || suffix.endsWith("/")) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return null;
        }

        return suffix.split("/");
    }

    private static Integer getId(HttpExchange exchange, String[] parts) throws IOException {
        int id;
        try {
            id = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return null;
        }

        if (id < 0) {
            sendEmptyResponse(HttpURLConnection.HTTP_NOT_FOUND, exchange);
            return null;
        }
        return id;
    }
}
