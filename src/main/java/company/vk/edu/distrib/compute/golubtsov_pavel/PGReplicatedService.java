package company.vk.edu.distrib.compute.golubtsov_pavel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;

public class PGReplicatedService implements ReplicatedService {
    private static final String ID_PARAM_PREFIX = "id=";
    private static final String ACK_PARAM = "ack=";
    private static final int DEFAULT_REPLICA_COUNT = 3;
    private static final int DEFAULT_ACK = 1;

    private final int port;
    private final int numberOfReplicas;
    private final List<ReplicaNode> replicas;
    private final HttpServer server;
    private final AtomicLong versionCounter;

    public PGReplicatedService(int port) throws IOException {
        this.port = port;
        this.numberOfReplicas = DEFAULT_REPLICA_COUNT;
        this.replicas = new ArrayList<>(numberOfReplicas);
        for (int i = 0; i < numberOfReplicas; i++) {
            replicas.add(new ReplicaNode(i));
        }
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newFixedThreadPool(numberOfReplicas * 4));
        this.versionCounter = new AtomicLong();
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(http -> {
            if (Objects.equals("GET", http.getRequestMethod())) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            String method = http.getRequestMethod();
            String query = http.getRequestURI().getQuery();
            String id = parseId(query);
            int ack = parseAck(query);
            validateAck(ack);

            if (Objects.equals("GET", method)) {
                handleGet(http, id, ack);
            } else if (Objects.equals("PUT", method)) {
                handlePut(http, id, ack);
            } else if (Objects.equals("DELETE", method)) {
                handleDelete(http, id, ack);
            } else {
                http.sendResponseHeaders(405, 0);
            }
        }));
    }

    private void handlePut(HttpExchange http, String id, int ack) throws IOException {
        byte[] body = http.getRequestBody().readAllBytes();
        ReplicaRecord record = new ReplicaRecord(body, versionCounter.incrementAndGet(), false);
        int successes = 0;

        for (ReplicaNode replica : replicas) {
            if (!replica.isEnabled()) {
                continue;
            }
            replica.upsert(id, record);
            successes++;
        }

        if (successes >= ack) {
            http.sendResponseHeaders(201, 0);
        } else {
            http.sendResponseHeaders(500, 0);
        }
    }

    private void handleDelete(HttpExchange http, String id, int ack) throws IOException {
        ReplicaRecord tombstone = new ReplicaRecord(null, versionCounter.incrementAndGet(), true);
        int successes = 0;

        for (ReplicaNode replica : replicas) {
            if (!replica.isEnabled()) {
                continue;
            }
            replica.delete(id, tombstone);
            successes++;
        }

        if (successes >= ack) {
            http.sendResponseHeaders(202, 0);
        } else {
            http.sendResponseHeaders(500, 0);
        }
    }

    private void handleGet(HttpExchange http, String id, int ack) throws IOException {
        int responses = 0;
        ReplicaRecord latest = null;

        for (ReplicaNode replica : replicas) {
            if (!replica.isEnabled()) {
                continue;
            }

            responses++;
            ReplicaRecord record = replica.getRecord(id);
            if (isNewer(record, latest)) {
                latest = record;
            }
        }

        if (responses < ack) {
            http.sendResponseHeaders(500, 0);
            return;
        }

        if (latest == null || latest.isDeleted()) {
            http.sendResponseHeaders(404, 0);
            return;
        }

        byte[] value = latest.getValue();
        http.sendResponseHeaders(200, value.length);
        http.getResponseBody().write(value);
    }

    private static boolean isNewer(ReplicaRecord candidate, ReplicaRecord current) {
        return candidate != null && (current == null || candidate.getTimestamp() > current.getTimestamp());
    }

    private void validateAck(int ack) {
        if (ack <= 0 || ack > numberOfReplicas) {
            throw new IllegalArgumentException("invalid ack");
        }
    }

    private static String parseId(String query) {
        if (query == null) {
            throw new IllegalArgumentException("bad query");
        }

        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith(ID_PARAM_PREFIX)) {
                String id = param.substring(ID_PARAM_PREFIX.length());
                if (id.isBlank()) {
                    throw new IllegalArgumentException("empty id");
                }
                return id;
            }
        }

        throw new IllegalArgumentException("missing id");
    }

    private static int parseAck(String query) {
        if (query == null) {
            throw new IllegalArgumentException("bad query");
        }

        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith(ACK_PARAM)) {
                String ack = param.substring(ACK_PARAM.length());
                if (ack.isBlank()) {
                    throw new IllegalArgumentException("empty ack");
                }
                try {
                    return Integer.parseInt(ack);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ack is not a number", e);
                }
            }
        }

        return DEFAULT_ACK;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicas.get(nodeId).disable();
    }

    @Override
    public void enableReplica(int nodeId) {
        replicas.get(nodeId).enable();
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException exp) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException exp) {
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException exp) {
                exchange.sendResponseHeaders(500, 0);
            }
            exchange.close();
        }
    }
}
