package company.vk.edu.distrib.compute.borodinavalera1996dev;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.borodinavalera1996dev.replication.ReplicaNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.GodClass")
public class KVServiceImpl implements KVService, ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    public static final String HTTP_METHOD_GET = "GET";
    public static final String HTTP_METHOD_PUT = "PUT";
    public static final String HTTP_METHOD_DELETE = "DELETE";
    public static final String PATH_STATUS = "/v0/status";
    public static final String PATH_ENTITY = "/v0/entity";
    private final Path path;

    protected HttpServer server;
    protected final int port;
    protected int numberOfReplications;
    protected List<ReplicaNode> replicaNodes = new ArrayList<>();

    public KVServiceImpl(int port, Path path, int numberOfReplications) throws IOException {
        this.path = path;
        this.port = port;
        this.numberOfReplications = numberOfReplications;
        initReplications();
    }

    private void initReplications() throws IOException {
        for (int i = 0; i < numberOfReplications; i++) {
            replicaNodes.add(new ReplicaNode(path, i));
        }
    }

    private void initServer() {
        createStatusContext();
        createKVContext();
    }

    protected void createKVContext() {
        server.createContext(PATH_ENTITY, wrapHandler(getKVHttpHandler()));
    }

    protected void createStatusContext() {
        server.createContext(PATH_STATUS, wrapHandler(getStatusHttpHandler()));
    }

    protected HttpHandler getKVHttpHandler() {
        return exchange -> {
            String requestMethod = exchange.getRequestMethod();
            Map<String, String> parms = getParms(exchange);
            String id = parms.get("id");
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            String ack = parms.get("ack");
            if (ack != null && Integer.parseInt(ack) > numberOfReplications) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            switch (requestMethod) {
                case HTTP_METHOD_GET:
                    byte[] value = callGet(id, ack);
                    exchange.sendResponseHeaders(200, value.length);
                    exchange.getResponseBody().write(value);
                    break;
                case HTTP_METHOD_PUT:
                    put(id, exchange.getRequestBody().readAllBytes(), ack);
                    exchange.sendResponseHeaders(201, -1);
                    break;
                case HTTP_METHOD_DELETE:
                    delete(id, ack);
                    exchange.sendResponseHeaders(202, -1);
                    break;
                default:
                    exchange.sendResponseHeaders(405, -1);
                    break;
            }
        };
    }

    private byte[] callGet(String id, String ackParam) throws IOException {
        int ack = ackParam == null ? 1 : Integer.parseInt(ackParam);
        FileStorage.Data freshest = getFreshData(id, ack);

        if (freshest == null) {
            log.error("Key {} not found on any of the responding replicas", id);
            throw new NoSuchElementException("Key " + id + " not found");
        }

        if (freshest.deleted()) {
            repairNodes(id, freshest);
            throw new NoSuchElementException("Key " + id + " was deleted");
        }

        repairNodes(id, freshest);
        return freshest.value();
    }

    private FileStorage.Data getFreshData(String id, int ack) throws IOException {
        int ackCount = 0;

        List<FileStorage.Data> result = new ArrayList<>(numberOfReplications);
        FileStorage.Data freshest = null;
        for (ReplicaNode replicaNode : replicaNodes) {
            if (replicaNode.getIsAlive().get()) {
                try {
                    result.add(replicaNode.getDao().get(id));
                    ackCount++;
                } catch (NoSuchElementException e) {
                    ackCount++;
                }
            }
            if (ackCount >= ack) {
                freshest = result.stream()
                        .max(Comparator.comparing(FileStorage.Data::time))
                        .orElseThrow();
                break;
            }
        }

        if (ackCount < ack) {
            throw new NotEnoughReplicasException(ackCount, ack);
        }
        return freshest;
    }

    private void repairNodes(String id, FileStorage.Data freshest) {
        for (ReplicaNode node : replicaNodes) {
            if (node.getIsAlive().get()) {
                try {
                    if (isNeedUpdate(id, freshest, node)) {
                        repairData(id, freshest, node);
                    }
                } catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.warn("Failed to repair node {} for key {}", node.getId(), id, e);
                    }
                }
            }
        }
    }

    private static void repairData(String id, FileStorage.Data freshest, ReplicaNode node) throws IOException {
        if (freshest.deleted()) {
            node.getDao().delete(id);
        } else {
            node.getDao().upsert(id, freshest);
        }
    }

    private static boolean isNeedUpdate(String id, FileStorage.Data freshest, ReplicaNode node) throws IOException {
        boolean needUpdate = false;
        try {
            FileStorage.Data localData = node.getDao().get(id);
            if (freshest.time().isAfter(localData.time())) {
                needUpdate = true;
            }
        } catch (NoSuchElementException e) {
            needUpdate = true;
        }
        return needUpdate;
    }

    private void delete(String id, String ackParam) throws IOException {
        int ack = ackParam == null ? 1 : Integer.parseInt(ackParam);
        int ackCount = 0;

        for (ReplicaNode replicaNode : replicaNodes) {
            if (replicaNode.getIsAlive().get()) {
                replicaNode.getDao().delete(id);
                ackCount++;
            }
            if (ackCount >= ack) {
                return;
            }
        }
        throw new NotEnoughReplicasException(ackCount, ack);
    }

    private void put(String id, byte[] bytes, String ackParam) throws IOException {
        int ack = ackParam == null ? 1 : Integer.parseInt(ackParam);
        int ackCount = 0;
        Instant current = Instant.now();

        for (ReplicaNode replicaNode : replicaNodes) {
            if (replicaNode.getIsAlive().get()) {
                replicaNode.getDao().upsert(id, new FileStorage.Data(bytes, current, false));
                ackCount++;
            }
            if (ackCount >= ack) {
                return;
            }
        }
        throw new NotEnoughReplicasException(ackCount, ack);
    }

    private static HttpHandler getStatusHttpHandler() {
        return exchange -> {
            String requestMethod = exchange.getRequestMethod();
            if (HTTP_METHOD_GET.equals(requestMethod)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        };
    }

    protected HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    sendError(exchange, 404, e.getMessage());
                } catch (IllegalArgumentException e) {
                    sendError(exchange, 400, e.getMessage());
                } catch (NotEnoughReplicasException e) {
                    sendError(exchange, 500, e.getMessage());
                } catch (Exception e) {
                    sendError(exchange, 503, e.getMessage());
                }
            }
        };
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String response = message == null ? "" : message;
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    protected static Map<String, String> getParms(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();

        return (query == null || query.isEmpty())
                ? Collections.emptyMap()
                : Arrays.stream(query.split("&"))
                .map(param -> param.split("=", 2))
                .collect(Collectors.toMap(
                        parts -> parts[0],
                        parts -> parts.length > 1 ? parts[1] : "",
                        (existing, replacement) -> existing
                ));
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            initServer();
            server.start();
            log.info("Started {}", port);
        } catch (IOException e) {
            log.error("Server is failed to start in {}", port, e);
            throw new UncheckedIOException("Server is failed to start", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping {}", port);
        server.stop(0);
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return numberOfReplications;
    }

    @Override
    public void disableReplica(int nodeId) {
        for (ReplicaNode replicaNode : replicaNodes) {
            if (replicaNode.getId() == nodeId) {
                replicaNode.getIsAlive().set(false);
            }
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        for (ReplicaNode replicaNode : replicaNodes) {
            if (replicaNode.getId() == nodeId) {
                replicaNode.getIsAlive().set(true);
            }
        }
    }

    public class NotEnoughReplicasException extends IOException {
        public NotEnoughReplicasException(int actual, int required) {
            super("Not enough replicas: " + actual + " acknowledge(s), but " + required + " required");
        }
    }
}
