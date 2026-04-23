package company.vk.edu.distrib.compute.ronshinvsevolod;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicatedKVService implements ReplicatedService {
    private HttpServer server;
    private final int servicePort;
    private final List<Dao<byte[]>> replicas;
    private final List<AtomicBoolean> isAlive;
    private final ReentrantLock lock = new ReentrantLock();
    private final ReentrantLock timestampLock = new ReentrantLock();
    private long lastTimestamp = System.currentTimeMillis();
    private ExecutorService executor;
    private boolean running;
    private static final int EXPECTED_PARTS = 2;
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_ERROR = 500;

    public ReplicatedKVService(int servicePort, List<Dao<byte[]>> replicas) {
        this.servicePort = servicePort;
        this.replicas = replicas;
        this.isAlive = new ArrayList<>(replicas.size());
        replicas.forEach(r -> this.isAlive.add(new AtomicBoolean(true)));
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        isAlive.get(nodeId).set(false);
    }

    @Override
    public void enableReplica(int nodeId) {
        isAlive.get(nodeId).set(true);
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (running) {
                throw new IllegalStateException("Already started");
            }
            try {
                executor = Executors.newCachedThreadPool();
                server = HttpServer.create(new InetSocketAddress(servicePort), 0);
                server.createContext("/v0/status", new StatusHandler());
                server.createContext("/v0/entity", new EntityHandler());
                server.setExecutor(executor);
                server.start();
                running = true;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to start HTTP server", e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (running) {
                server.stop(0);
                if (executor != null) {
                    executor.shutdownNow();
                }
                running = false;
            }
        } finally {
            lock.unlock();
        }
    }

    private static final class Record {
        private final long timestamp;
        private final boolean deleted;
        private final byte[] data;
        private static final Record EMPTY_RECORD = new Record(0, true, null);

        Record(long timestamp, boolean deleted, byte[] data) {
            this.timestamp = timestamp;
            this.deleted = deleted;
            this.data = data != null ? data.clone() : null;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public byte[] getData() {
            return data != null ? data.clone() : null;
        }

        byte[] serialize() {
            int len = 9 + (data == null ? 0 : data.length);
            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putLong(timestamp);
            buf.put((byte) (deleted ? 1 : 0));
            if (data != null) {
                buf.put(data);
            }
            return buf.array();
        }

        static Record deserialize(byte[] raw) {
            if (raw == null || raw.length < 9) {
                return new Record(0, true, null);
            }
            ByteBuffer buf = ByteBuffer.wrap(raw);
            long ts = buf.getLong();
            boolean del = buf.get() == 1;
            byte[] d = new byte[raw.length - 9];
            buf.get(d);
            return new Record(ts, del, d);
        }
    }

    private static final class QuorumWaiter {
        private final ReentrantLock waiterLock = new ReentrantLock();
        private final java.util.concurrent.locks.Condition condition = waiterLock.newCondition();
        private int successes;
        private int errors;
        private Record latestRecord;

        void onSuccess(Record record) {
            waiterLock.lock();
            try {
                successes++;
                if (record != null && (latestRecord == null
                        || record.getTimestamp() > latestRecord.getTimestamp())) {
                    latestRecord = record;
                }
                condition.signalAll();
            } finally {
                waiterLock.unlock();
            }
        }

        void onError() {
            waiterLock.lock();
            try {
                errors++;
                condition.signalAll();
            } finally {
                waiterLock.unlock();
            }
        }

        void await(int ack, int total) throws InterruptedException {
            waiterLock.lock();
            try {
                long deadline = System.currentTimeMillis() + 4000;
                while (successes < ack && (successes + errors) < total) {
                    long waitTime = deadline - System.currentTimeMillis();
                    if (waitTime <= 0) {
                        break;
                    }
                    condition.await(waitTime, java.util.concurrent.TimeUnit.MILLISECONDS);
                }
            } finally {
                waiterLock.unlock();
            }
        }

        int getSuccesses() {
            return successes;
        }

        Record getLatestRecord() {
            return latestRecord;
        }
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if (METHOD_GET.equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(HTTP_OK, -1);
                } else {
                    exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
                }
            } finally {
                exchange.close();
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        private int parseAck(String query) {
            String ackStr = extractParam(query, ACK_PARAM);
            return ackStr != null ? Integer.parseInt(ackStr) : 1;
        }

        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String method = exchange.getRequestMethod();
                String query = exchange.getRequestURI().getQuery();
                String id = extractParam(query, ID_PARAM);

                if (id == null || id.isEmpty()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }

                int ack = parseAck(query);

                if (ack > replicas.size()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }

                switch (method) {
                    case METHOD_GET:
                        handleGet(exchange, id, ack);
                        break;
                    case METHOD_PUT:
                        handlePut(exchange, id, ack);
                        break;
                    case METHOD_DELETE:
                        handleDelete(exchange, id, ack);
                        break;
                    default:
                        exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
                        break;
                }
            } catch (NumberFormatException e) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
            } finally {
                exchange.close();
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            QuorumWaiter waiter = new QuorumWaiter();
            int tasksSubmitted = submitReadTasks(id, waiter);

            try {
                waiter.await(ack, tasksSubmitted);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            if (waiter.getSuccesses() >= ack) {
                if (waiter.getLatestRecord() == null || waiter.getLatestRecord().isDeleted()) {
                    exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
                } else {
                    byte[] data = waiter.getLatestRecord().getData();
                    exchange.sendResponseHeaders(HTTP_OK, data.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(data);
                    }
                }
            } else {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            byte[] body;
            try (InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }
            long ts = nextTimestamp();
            Record record = new Record(ts, false, body);
            executeWriteQuorum(exchange, id, ack, record, HTTP_CREATED);
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            long ts = nextTimestamp();
            Record record = new Record(ts, true, null);
            executeWriteQuorum(exchange, id, ack, record, HTTP_ACCEPTED);
        }

        private int submitReadTasks(String id, QuorumWaiter waiter) {
            int submitted = 0;
            for (int i = 0; i < replicas.size(); i++) {
                if (isAlive.get(i).get()) {
                    final int idx = i;
                    submitted++;
                    executor.submit(() -> {
                        try {
                            byte[] raw = replicas.get(idx).get(id);
                            waiter.onSuccess(Record.deserialize(raw));
                        } catch (NoSuchElementException e) {
                            waiter.onSuccess(Record.EMPTY_RECORD);
                        } catch (Exception e) {
                            waiter.onError();
                        }
                    });
                }
            }
            return submitted;
        }

        private void executeWriteQuorum(HttpExchange exchange, String id, int ack, Record record, int successCode) 
                throws IOException {
            QuorumWaiter waiter = new QuorumWaiter();
            byte[] payload = record.serialize();
            int submitted = 0;

            for (int i = 0; i < replicas.size(); i++) {
                if (isAlive.get(i).get()) {
                    final int idx = i;
                    submitted++;
                    executor.submit(() -> {
                        try {
                            replicas.get(idx).upsert(id, payload);
                            waiter.onSuccess(null);
                        } catch (Exception e) {
                            waiter.onError();
                        }
                    });
                }
            }

            try {
                waiter.await(ack, submitted);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
                return;
            }

            if (waiter.getSuccesses() >= ack) {
                exchange.sendResponseHeaders(successCode, -1);
            } else {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private String extractParam(String query, String paramName) {
            if (query == null) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == EXPECTED_PARTS && paramName.equals(pair[0])) {
                    return pair[1];
                }
            }
            return null;
        }

        private long nextTimestamp() {
            timestampLock.lock();
            try {
                long now = System.currentTimeMillis();
                if (now > lastTimestamp) {
                    lastTimestamp = now;
                } else {
                    lastTimestamp++;
                }
                return lastTimestamp;
            } finally {
                timestampLock.unlock();
            }
        }
    }
}
