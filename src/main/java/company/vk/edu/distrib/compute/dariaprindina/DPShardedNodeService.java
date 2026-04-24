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
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final int DEFAULT_ACK = 1;
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
            if (Objects.equals("GET", exchange.getRequestMethod())) {
                sendResponse(exchange, 200, null);
            } else {
                sendResponse(exchange, 405, null);
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
        if ("GET".equals(method)) {
            handleReplicatedGet(exchange, id, ack, replicas);
            return;
        }
        if ("PUT".equals(method)) {
            final byte[] body;
            try (var requestBody = exchange.getRequestBody()) {
                body = requestBody.readAllBytes();
            }
            handleReplicatedWrite(exchange, id, body, ack, replicas, method);
            return;
        }
        if ("DELETE".equals(method)) {
            handleReplicatedWrite(exchange, id, null, ack, replicas, method);
            return;
        }
        sendResponse(exchange, 405, null);
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
            final OperationResult result = executeOnReplica(replicaEndpoint, "GET", id, null);
            if (result.statusCode == 200) {
                found++;
                if (value == null) {
                    value = result.body;
                }
            } else if (result.statusCode == 404) {
                missing++;
            }
        }

        if (found >= ack) {
            sendResponse(exchange, 200, value);
            return;
        }
        if (missing >= ack) {
            sendResponse(exchange, 404, null);
            return;
        }
        sendResponse(exchange, 504, null);
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
            final int successCode = "PUT".equals(method) ? 201 : 202;
            sendResponse(exchange, successCode, null);
            return;
        }
        sendResponse(exchange, 504, null);
    }

    private void handleLocally(HttpExchange exchange, String id) throws IOException {
        final String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
            final byte[] value = dao.get(id);
            sendResponse(exchange, 200, value);
            return;
        }
        if ("PUT".equals(method)) {
            try (var requestBody = exchange.getRequestBody()) {
                dao.upsert(id, requestBody.readAllBytes());
            }
            sendResponse(exchange, 201, null);
            return;
        }
        if ("DELETE".equals(method)) {
            dao.delete(id);
            sendResponse(exchange, 202, null);
            return;
        }
        sendResponse(exchange, 405, null);
    }

    private OperationResult executeOnReplica(String replicaEndpoint, String method, String id, byte[] body) {
        if (localEndpoint.equals(replicaEndpoint)) {
            return executeLocally(method, id, body);
        }
        return executeRemotely(replicaEndpoint, method, id, body);
    }

    private OperationResult executeLocally(String method, String id, byte[] body) {
        try {
            if ("GET".equals(method)) {
                return new OperationResult(200, dao.get(id));
            }
            if ("PUT".equals(method)) {
                dao.upsert(id, body);
                return new OperationResult(201, null);
            }
            if ("DELETE".equals(method)) {
                dao.delete(id);
                return new OperationResult(202, null);
            }
            return new OperationResult(405, null);
        } catch (NoSuchElementException e) {
            return new OperationResult(404, null);
        } catch (IllegalArgumentException e) {
            return new OperationResult(400, null);
        } catch (IOException e) {
            return new OperationResult(500, null);
        }
    }

    private OperationResult executeRemotely(String replicaEndpoint, String method, String id, byte[] body) {
        final String targetUrl = replicaEndpoint + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(targetUrl))
            .timeout(PROXY_TIMEOUT)
            .header(INTERNAL_REPLICA_HEADER, "true");

        if ("GET".equals(method)) {
            requestBuilder.GET();
        } else if ("DELETE".equals(method)) {
            requestBuilder.DELETE();
        } else if ("PUT".equals(method)) {
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        } else {
            return new OperationResult(405, null);
        }

        final HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            return new OperationResult(504, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new OperationResult(504, null);
        }
        return new OperationResult(response.statusCode(), response.body());
    }

    private static boolean isSuccessfulWriteStatus(int statusCode, String method) {
        return ("PUT".equals(method) && statusCode == 201)
            || ("DELETE".equals(method) && statusCode == 202);
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
        if (ack < 1) {
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
                sendResponse(exchange, 400, null);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, null);
            } catch (IOException e) {
                sendResponse(exchange, 500, null);
            }
        }
    }
}
