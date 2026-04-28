package company.vk.edu.distrib.compute.dariaprindina;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("PMD.GodClass")
public class DPShardedNodeService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(DPShardedNodeService.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final int MIN_ACK = 1;
    private static final int DEFAULT_ACK = 1;
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final int STATUS_GATEWAY_TIMEOUT = 504;
    private static final String INTERNAL_REPLICA_HEADER = "X-DP-Internal-Replica";
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final String localEndpoint;
    private final HttpServer server;
    private final HttpClient httpClient;
    private final Dao<byte[]> dao;
    private final DPShardSelector shardSelector;
    private final int replicationFactor;

    public DPShardedNodeService(
        String localEndpoint,
        Dao<byte[]> dao,
        DPShardSelector shardSelector,
        int replicationFactor
    ) throws IOException {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.dao = Objects.requireNonNull(dao, "dao");
        this.shardSelector = Objects.requireNonNull(shardSelector, "shardSelector");
        this.replicationFactor = replicationFactor;
        this.httpClient = HttpClient.newHttpClient();
        this.server = createHttpServer(localEndpoint);
        initServer();
    }

    private static HttpServer createHttpServer(String endpoint) throws IOException {
        final URI uri = URI.create(endpoint);
        return HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(exchange -> {
            if (Objects.equals(METHOD_GET, exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_OK, null);
            } else {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, null);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(exchange -> {
            final Map<String, String> queryParams = parseQueryParams(exchange.getRequestURI().getQuery());
            final String id = parseId(queryParams);
            if (isInternalReplicaRequest(exchange)) {
                handleLocally(exchange, id);
                return;
            }
            final int ack = parseAck(queryParams);
            final List<String> replicas = shardSelector.replicasForKey(id, replicationFactor);
            if (ack > replicas.size()) {
                throw new IllegalArgumentException("ack should not be greater than replicas");
            }
            routeReplicated(exchange, id, ack, replicas);
        }));
    }

    private void routeReplicated(HttpExchange exchange, String id, int ack, List<String> replicas) throws IOException {
        final String method = exchange.getRequestMethod();
        if (METHOD_GET.equals(method)) {
            handleReplicatedGet(exchange, id, ack, replicas);
            return;
        }
        if (METHOD_PUT.equals(method)) {
            final byte[] body;
            try (var requestBody = exchange.getRequestBody()) {
                body = requestBody.readAllBytes();
            }
            handleReplicatedWrite(exchange, id, body, ack, replicas, method);
            return;
        }
        if (METHOD_DELETE.equals(method)) {
            handleReplicatedWrite(exchange, id, null, ack, replicas, method);
            return;
        }
        sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, null);
    }

    private void handleReplicatedGet(
        HttpExchange exchange,
        String id,
        int ack,
        List<String> replicas
    ) throws IOException {
        int found = 0;
        int missing = 0;
        byte[] value = null;
        for (String replicaEndpoint : replicas) {
            final OperationResult result = executeOnReplica(replicaEndpoint, METHOD_GET, id, null);
            if (result.statusCode == STATUS_OK) {
                found++;
                if (value == null) {
                    value = result.body;
                }
            } else if (result.statusCode == STATUS_NOT_FOUND) {
                missing++;
            }
        }

        if (found >= ack) {
            sendResponse(exchange, STATUS_OK, value);
            return;
        }
        if (missing >= ack) {
            sendResponse(exchange, STATUS_NOT_FOUND, null);
            return;
        }
        sendResponse(exchange, STATUS_GATEWAY_TIMEOUT, null);
    }

    private void handleReplicatedWrite(
        HttpExchange exchange,
        String id,
        byte[] body,
        int ack,
        List<String> replicas,
        String method
    ) throws IOException {
        int successfulAcks = 0;
        for (String replicaEndpoint : replicas) {
            final OperationResult result = executeOnReplica(replicaEndpoint, method, id, body);
            if (isSuccessfulWriteStatus(result.statusCode, method)) {
                successfulAcks++;
            }
        }

        if (successfulAcks >= ack) {
            final int successCode = METHOD_PUT.equals(method) ? STATUS_CREATED : STATUS_ACCEPTED;
            sendResponse(exchange, successCode, null);
            return;
        }
        sendResponse(exchange, STATUS_GATEWAY_TIMEOUT, null);
    }

    private void handleLocally(HttpExchange exchange, String id) throws IOException {
        final String method = exchange.getRequestMethod();
        if (METHOD_GET.equals(method)) {
            final byte[] value = dao.get(id);
            sendResponse(exchange, STATUS_OK, value);
            return;
        }
        if (METHOD_PUT.equals(method)) {
            try (var requestBody = exchange.getRequestBody()) {
                dao.upsert(id, requestBody.readAllBytes());
            }
            sendResponse(exchange, STATUS_CREATED, null);
            return;
        }
        if (METHOD_DELETE.equals(method)) {
            dao.delete(id);
            sendResponse(exchange, STATUS_ACCEPTED, null);
            return;
        }
        sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, null);
    }

    private OperationResult executeOnReplica(String replicaEndpoint, String method, String id, byte[] body) {
        if (localEndpoint.equals(replicaEndpoint)) {
            return executeLocally(method, id, body);
        }
        return executeRemotely(replicaEndpoint, method, id, body);
    }

    private OperationResult executeLocally(String method, String id, byte[] body) {
        try {
            if (METHOD_GET.equals(method)) {
                return new OperationResult(STATUS_OK, dao.get(id));
            }
            if (METHOD_PUT.equals(method)) {
                dao.upsert(id, body);
                return new OperationResult(STATUS_CREATED, null);
            }
            if (METHOD_DELETE.equals(method)) {
                dao.delete(id);
                return new OperationResult(STATUS_ACCEPTED, null);
            }
            return new OperationResult(STATUS_METHOD_NOT_ALLOWED, null);
        } catch (NoSuchElementException e) {
            return new OperationResult(STATUS_NOT_FOUND, null);
        } catch (IllegalArgumentException e) {
            return new OperationResult(STATUS_BAD_REQUEST, null);
        } catch (IOException e) {
            return new OperationResult(STATUS_INTERNAL_ERROR, null);
        }
    }

    private OperationResult executeRemotely(String replicaEndpoint, String method, String id, byte[] body) {
        final String targetUrl = replicaEndpoint + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(targetUrl))
            .timeout(PROXY_TIMEOUT)
            .header(INTERNAL_REPLICA_HEADER, "true");

        if (METHOD_GET.equals(method)) {
            requestBuilder.GET();
        } else if (METHOD_DELETE.equals(method)) {
            requestBuilder.DELETE();
        } else if (METHOD_PUT.equals(method)) {
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        } else {
            return new OperationResult(STATUS_METHOD_NOT_ALLOWED, null);
        }

        final HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            return new OperationResult(STATUS_GATEWAY_TIMEOUT, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new OperationResult(STATUS_GATEWAY_TIMEOUT, null);
        }
        return new OperationResult(response.statusCode(), response.body());
    }

    private static boolean isSuccessfulWriteStatus(int statusCode, String method) {
        return (METHOD_PUT.equals(method) && statusCode == STATUS_CREATED)
            || (METHOD_DELETE.equals(method) && statusCode == STATUS_ACCEPTED);
    }

    private static boolean isInternalReplicaRequest(HttpExchange exchange) {
        return "true".equalsIgnoreCase(exchange.getRequestHeaders().getFirst(INTERNAL_REPLICA_HEADER));
    }

    private static int parseAck(Map<String, String> queryParams) {
        final String ackRaw = queryParams.get(ACK_PARAM);
        if (ackRaw == null) {
            return DEFAULT_ACK;
        }
        final int ack = Integer.parseInt(ackRaw);
        if (ack < MIN_ACK) {
            throw new IllegalArgumentException("ack should be positive");
        }
        return ack;
    }

    private static String parseId(Map<String, String> queryParams) {
        final String id = queryParams.get(ID_PARAM);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("bad query");
        }
        return id;
    }

    private static Map<String, String> parseQueryParams(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("bad query");
        }
        final Map<String, String> params = new ConcurrentHashMap<>();
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            final String[] parts = pair.split("=", 2);
            final String rawKey = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            final String rawValue = parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            params.put(rawKey, rawValue);
        }
        return params;
    }

    @Override
    public void start() {
        server.start();
        log.info("Node started. endpoint={}", localEndpoint);
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Node stopped. endpoint={}", localEndpoint);
    }

    private static void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        final byte[] responseBody = body == null ? new byte[0] : body;
        exchange.sendResponseHeaders(code, responseBody.length);
        if (responseBody.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(responseBody);
            }
            return;
        }
        exchange.getResponseBody().close();
    }

    private record OperationResult(int statusCode, byte[] body) {
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
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, STATUS_BAD_REQUEST, null);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, STATUS_NOT_FOUND, null);
            } catch (IOException e) {
                sendResponse(exchange, STATUS_INTERNAL_ERROR, null);
            }
        }
    }
}
