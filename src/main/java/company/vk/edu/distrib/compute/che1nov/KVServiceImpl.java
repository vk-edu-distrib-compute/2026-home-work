package company.vk.edu.distrib.compute.che1nov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.che1nov.cluster.ClusterProxyClient;
import company.vk.edu.distrib.compute.che1nov.cluster.ShardRouter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@SuppressWarnings({
        "PMD.GodClass",
        "PMD.CyclomaticComplexity"
})
public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final int BAD_REQUEST = 400;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_FOUND = 404;
    private static final int UNAVAILABLE = 503;
    private static final int GATEWAY_TIMEOUT = 504;
    private static final int DEFAULT_ACK = 1;

    private final Dao<byte[]> dao;
    private final String localEndpoint;
    private final ShardRouter shardRouter;
    private final ClusterProxyClient proxyClient;
    private final int replicationFactor;

    private final HttpServer server;
    private final InetSocketAddress listenAddress;
    private boolean started;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, null, null, null, 1);
    }

    public KVServiceImpl(
            int port,
            Dao<byte[]> dao,
            String localEndpoint,
            ShardRouter shardRouter,
            ClusterProxyClient proxyClient,
            int replicationFactor
    ) throws IOException {
        this.dao = validateDao(dao);
        this.localEndpoint = localEndpoint;
        this.shardRouter = shardRouter;
        this.proxyClient = proxyClient;
        this.replicationFactor = validateReplicationFactor(replicationFactor);
        this.listenAddress = createListenAddress(port);
        this.server = HttpServer.create();
        createContexts();
    }

    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>
     * May be called only once.
     */
    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }

        try {
            server.bind(listenAddress, 0);
            if (log.isDebugEnabled()) {
                log.debug("HTTP server bound to {}", server.getAddress());
            }
            server.start();
            if (log.isDebugEnabled()) {
                log.debug("HTTP server started on {}", server.getAddress());
            }
            started = true;
        } catch (IOException e) {
            log.error("Failed to start HTTP server on {}", listenAddress, e);
            throw new UncheckedIOException("Failed to start HTTP server", e);
        }
    }

    /**
     * Stop listening and free all the resources.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service is not started");
        }

        server.stop(0);
        started = false;
        if (log.isDebugEnabled()) {
            log.debug("HTTP server stopped");
        }

        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close DAO", e);
        }
    }

    private void createContexts() {
        server.createContext("/v0/status", wrapHandler(this::handleStatus));
        server.createContext("/v0/entity", wrapHandler(this::handleEntity));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, GET_METHOD)) {
            exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
            return;
        }
        exchange.sendResponseHeaders(OK, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQueryParams(exchange);
        String requestMethod = exchange.getRequestMethod();
        if (!isSupportedMethod(requestMethod)) {
            exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
            return;
        }

        String id = params.get("id");
        if (Objects.isNull(id) || id.isBlank()) {
            exchange.sendResponseHeaders(BAD_REQUEST, -1);
            return;
        }

        boolean internalRequest = isInternalProxyRequest(exchange);
        if (internalRequest || !isClusterMode()) {
            byte[] requestBody = readRequestBodyIfNeeded(exchange, requestMethod);
            ReplicaResponse response = executeLocal(requestMethod, id, requestBody);
            writeResponse(exchange, response.statusCode(), response.body());
            return;
        }

        int ack = resolveAck(params, DEFAULT_ACK);
        if (ack > replicationFactor) {
            exchange.sendResponseHeaders(BAD_REQUEST, -1);
            return;
        }

        byte[] requestBody = readRequestBodyIfNeeded(exchange, requestMethod);
        List<String> replicaEndpoints = shardRouter.endpointsByKey(id, replicationFactor);
        ReplicaDecision decision;
        try {
            decision = executeReplicated(requestMethod, id, requestBody, ack, replicaEndpoints);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Replication request interrupted", e);
        }
        writeResponse(exchange, decision.statusCode(), decision.body());
    }

    private static Map<String, String> parseQueryParams(HttpExchange exchange) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        ConcurrentMap<String, String> params = new ConcurrentHashMap<>();

        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }

        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);

            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";

            params.put(key, value);
        }

        return params;
    }

    private HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Handling {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
                    }
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    sendError(exchange, NOT_FOUND, e.getMessage());
                } catch (IllegalArgumentException e) {
                    sendError(exchange, BAD_REQUEST, e.getMessage());
                } catch (Exception e) {
                    sendError(exchange, UNAVAILABLE, e.getMessage());
                }
            }
        };
    }

    private static int resolveAck(Map<String, String> params, int defaultAck) {
        String rawAck = params.get("ack");
        if (rawAck == null || rawAck.isBlank()) {
            return defaultAck;
        }

        int ack = Integer.parseInt(rawAck);
        if (ack <= 0) {
            throw new IllegalArgumentException("ack must be positive");
        }
        return ack;
    }

    private boolean isClusterMode() {
        return localEndpoint != null && shardRouter != null && proxyClient != null;
    }

    private static boolean isInternalProxyRequest(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst(ClusterProxyClient.INTERNAL_REQUEST_HEADER);
        return ClusterProxyClient.INTERNAL_REQUEST_VALUE.equalsIgnoreCase(header);
    }

    private static boolean isSupportedMethod(String requestMethod) {
        return GET_METHOD.equals(requestMethod)
                || PUT_METHOD.equals(requestMethod)
                || DELETE_METHOD.equals(requestMethod);
    }

    private static byte[] readRequestBodyIfNeeded(HttpExchange exchange, String requestMethod) throws IOException {
        if (PUT_METHOD.equals(requestMethod)) {
            return exchange.getRequestBody().readAllBytes();
        }
        return new byte[0];
    }

    private ReplicaDecision executeReplicated(
            String requestMethod,
            String id,
            byte[] requestBody,
            int ack,
            List<String> replicaEndpoints
    ) throws IOException, InterruptedException {
        int acknowledged = 0;
        List<ReplicaResponse> successfulReads = new ArrayList<>();
        int requiredSuccessStatus = successStatusForMethod(requestMethod);

        for (String endpoint : replicaEndpoints) {
            ReplicaResponse response = endpoint.equals(localEndpoint)
                    ? executeLocal(requestMethod, id, requestBody)
                    : executeRemote(requestMethod, endpoint, id, requestBody);

            if (isAcknowledged(requestMethod, response.statusCode(), requiredSuccessStatus)) {
                acknowledged++;
                if (GET_METHOD.equals(requestMethod)) {
                    successfulReads.add(response);
                }
            }
        }

        if (acknowledged < ack) {
            return new ReplicaDecision(GATEWAY_TIMEOUT, null);
        }

        if (GET_METHOD.equals(requestMethod)) {
            return chooseReadResponse(successfulReads);
        }
        return new ReplicaDecision(requiredSuccessStatus, null);
    }

    private ReplicaResponse executeLocal(String requestMethod, String id, byte[] requestBody) {
        try {
            return switch (requestMethod) {
                case GET_METHOD -> {
                    byte[] value = dao.get(id);
                    yield new ReplicaResponse(OK, value);
                }
                case PUT_METHOD -> {
                    dao.upsert(id, requestBody);
                    yield new ReplicaResponse(CREATED, null);
                }
                case DELETE_METHOD -> {
                    dao.delete(id);
                    yield new ReplicaResponse(ACCEPTED, null);
                }
                default -> new ReplicaResponse(METHOD_NOT_ALLOWED, null);
            };
        } catch (NoSuchElementException e) {
            return new ReplicaResponse(NOT_FOUND, null);
        } catch (Exception e) {
            return new ReplicaResponse(UNAVAILABLE, null);
        }
    }

    private ReplicaResponse executeRemote(
            String requestMethod,
            String targetEndpoint,
            String key,
            byte[] requestBody
    ) throws IOException, InterruptedException {
        ConcurrentMap<String, String> queryParams = new ConcurrentHashMap<>();
        queryParams.put("id", key);
        var response = proxyClient.forward(
                requestMethod,
                targetEndpoint,
                ENTITY_PATH,
                queryParams,
                true,
                requestBody
        );
        return new ReplicaResponse(response.statusCode(), response.body());
    }

    private static int successStatusForMethod(String requestMethod) {
        return switch (requestMethod) {
            case GET_METHOD -> OK;
            case PUT_METHOD -> CREATED;
            case DELETE_METHOD -> ACCEPTED;
            default -> METHOD_NOT_ALLOWED;
        };
    }

    private static boolean isAcknowledged(String requestMethod, int statusCode, int requiredSuccessStatus) {
        if (GET_METHOD.equals(requestMethod)) {
            return statusCode == OK || statusCode == NOT_FOUND;
        }
        return statusCode == requiredSuccessStatus;
    }

    private static ReplicaDecision chooseReadResponse(List<ReplicaResponse> responses) {
        for (ReplicaResponse response : responses) {
            if (response.statusCode() == OK) {
                return new ReplicaDecision(OK, response.body());
            }
        }
        return new ReplicaDecision(NOT_FOUND, null);
    }

    private void writeResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        if (body == null || body.length == 0) {
            exchange.sendResponseHeaders(statusCode, -1);
            return;
        }

        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String response = message == null ? "" : message;
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        writeResponse(exchange, statusCode, bytes);
    }

    private static Dao<byte[]> validateDao(Dao<byte[]> dao) {
        if (dao == null) {
            throw new IllegalArgumentException("dao must not be null");
        }

        return dao;
    }

    private static int validateReplicationFactor(int replicationFactor) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be positive");
        }
        return replicationFactor;
    }

    @SuppressFBWarnings(
            value = "URLCONNECTION_SSRF_FD",
            justification = "Local server bind only"
    )
    private static InetSocketAddress createListenAddress(int port) {
        validatePort(port);
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    private static void validatePort(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
    }

    private record ReplicaResponse(int statusCode, byte[] body) {
    }

    private record ReplicaDecision(int statusCode, byte[] body) {
    }
}
