package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.FileDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

import static company.vk.edu.distrib.compute.gavrilova_ekaterina.RequestParserHelper.extractAck;
import static company.vk.edu.distrib.compute.gavrilova_ekaterina.RequestParserHelper.extractId;

public class ReplicationKVServiceImpl implements KVService {

    private static final String LOCALHOST = "http://localhost:";
    private static final String INTERNAL_REQUEST_HEADER = "X-Internal";
    private static final ExecutorService REPLICA_EXECUTOR = Executors.newCachedThreadPool();
    private static final Logger log = LoggerFactory.getLogger(ReplicationKVServiceImpl.class);
    private final HttpServer server;
    private final Dao<byte[]> localDao;
    private final List<String> clusterNodes;
    private final int replicationFactor;
    private final ReplicaClient replicaClient;

    public ReplicationKVServiceImpl(int port, List<String> clusterNodes, int replicationFactor) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.clusterNodes = clusterNodes;
        this.replicationFactor = replicationFactor;
        this.localDao = new FileDao(Path.of("storage-" + port));
        String selfUrl = LOCALHOST + port;
        this.replicaClient = new ReplicaClient(selfUrl, localDao);

        initServer();
    }

    @Override
    public void start() {
        server.start();
        log.info("ReplicationKVService started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("ReplicationKVService stopped");
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        } else {
            createContexts();
            log.info("Server is initialized");
        }
    }

    private void createContexts() {
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, "GET")) {
            sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            return;
        }
        sendResponse(exchange, 200, "OK".getBytes());
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            String id = validateId(exchange);

            if (isInternalRequest(exchange)) {
                handleInternalRequest(exchange, id);
                return;
            }

            int ack = validateAck(exchange);

            dispatch(exchange, id, ack);

        } catch (IOException e) {
            log.error("Error handling /v0/entity", e);
            sendResponse(exchange, 500, "Internal Server Error".getBytes());
        }
    }

    private String validateId(HttpExchange exchange) throws IOException {
        String id = extractId(exchange);
        if (id == null) {
            sendResponse(exchange, 400, "Missing id parameter".getBytes());
        }
        return id;
    }

    private int validateAck(HttpExchange exchange) throws IOException {
        int ack = extractAck(exchange, replicationFactor);
        if (ack <= 0 || ack > replicationFactor) {
            sendResponse(exchange, 400, "Invalid ack".getBytes());
        }
        return ack;
    }

    private void dispatch(HttpExchange exchange, String id, int ack) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id, ack);
            case "PUT" -> handlePut(exchange, id, ack);
            case "DELETE" -> handleDelete(exchange, id, ack);
            default -> sendResponse(exchange, 405, new byte[0]);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        List<ReplicaResponse> responses = collectFromReplicas(
                id,
                ack,
                node -> replicaClient.get(node, id)
        );
        long responded = responses.stream()
                .filter(r -> r.status() != 503)
                .count();
        if (responded < ack) {
            sendResponse(exchange, 504, new byte[0]);
            return;
        }
        for (ReplicaResponse r : responses) {
            if (r.status() == HttpURLConnection.HTTP_OK) {
                sendResponse(exchange, 200, r.body());
                return;
            }
        }
        sendResponse(exchange, 404, new byte[0]);
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        List<ReplicaResponse> responses = collectFromReplicas(
                id,
                ack,
                node -> replicaClient.put(node, id, body)
        );
        long success = responses.stream()
                .filter(ReplicaResponse::isSuccess)
                .count();
        if (success >= ack) {
            sendResponse(exchange, 201, new byte[0]);
        } else {
            sendResponse(exchange, 504, new byte[0]);
        }
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        List<ReplicaResponse> responses = collectFromReplicas(
                id,
                ack,
                node -> replicaClient.delete(node, id)
        );
        long success = responses.stream()
                .filter(ReplicaResponse::isSuccess)
                .count();
        if (success >= ack) {
            sendResponse(exchange, 202, new byte[0]);
        } else {
            sendResponse(exchange, 504, new byte[0]);
        }
    }

    private List<String> selectReplicas(String key) {
        int start = Math.abs(key.hashCode()) % clusterNodes.size();
        List<String> result = new ArrayList<>();

        for (int i = 0; i < replicationFactor; i++) {
            result.add(clusterNodes.get((start + i) % clusterNodes.size()));
        }
        return result;
    }

    private void handleInternalRequest(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> {
                try {
                    byte[] value = localDao.get(id);
                    sendResponse(exchange, 200, value);
                } catch (Exception e) {
                    sendResponse(exchange, 404, new byte[0]);
                }
            }
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                localDao.upsert(id, body);
                sendResponse(exchange, 201, new byte[0]);
            }
            case "DELETE" -> {
                localDao.delete(id);
                sendResponse(exchange, 202, new byte[0]);
            }
            default -> sendResponse(exchange, 405, new byte[0]);
        }
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        String headerValue = exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        return "true".equalsIgnoreCase(headerValue);
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private List<ReplicaResponse> collectFromReplicas(String id, int ack, Function<String, ReplicaResponse> action) {
        List<ReplicaResponse> responses = new ArrayList<>();

        for (String node : selectReplicas(id)) {

            Future<ReplicaResponse> future = REPLICA_EXECUTOR.submit(() -> {
                log.info("Sending request to replica {}", node);
                return action.apply(node);
            });

            try {
                ReplicaResponse response = future.get(1200, TimeUnit.MILLISECONDS);
                responses.add(response);

            } catch (TimeoutException e) {
                log.warn("Replica {} timeout", node);
                future.cancel(true);
                responses.add(new ReplicaResponse(503, null));

            } catch (Exception e) {
                log.warn("Replica {} failed", node, e);
                responses.add(new ReplicaResponse(503, null));
            }

            long success = responses.stream()
                    .filter(r -> r.isSuccess() || r.status() == 404)
                    .count();

            if (success >= ack) {
                log.info("Quorum achieved {}/{}", success, ack);
                break;
            }
        }
        return responses;
    }

}
