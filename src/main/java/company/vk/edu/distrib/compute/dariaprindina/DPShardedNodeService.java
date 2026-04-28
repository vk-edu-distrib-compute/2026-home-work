package company.vk.edu.distrib.compute.dariaprindina;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.dariaprindina.grpc.DPInternalKvServiceGrpc;
import company.vk.edu.distrib.compute.dariaprindina.grpc.InternalRequest;
import company.vk.edu.distrib.compute.dariaprindina.grpc.InternalResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({
    "PMD.GodClass",
    "PMD.CouplingBetweenObjects",
    "PMD.ExcessiveImports"
})
public class DPShardedNodeService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(DPShardedNodeService.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final String GRPC_PORT_PARAM = "grpcPort";
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
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final String localEndpoint;
    private final HttpServer httpServer;
    private final Server grpcServer;
    private final Dao<byte[]> dao;
    private final DPShardSelector shardSelector;
    private final int replicationFactor;
    private final Map<String, ManagedChannel> channelByEndpoint;

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
        this.channelByEndpoint = new ConcurrentHashMap<>();
        this.httpServer = createHttpServer(localEndpoint);
        this.grpcServer = createGrpcServer(localEndpoint);
        initHttpServer();
    }

    private static HttpServer createHttpServer(String endpoint) throws IOException {
        final URI uri = URI.create(endpoint);
        return HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
    }

    private Server createGrpcServer(String endpoint) {
        final int grpcPort = parseGrpcPort(endpoint);
        return ServerBuilder.forPort(grpcPort)
            .addService(new InternalGrpcService())
            .build();
    }

    private void initHttpServer() {
        httpServer.createContext("/v0/status", new ErrorHttpHandler(exchange -> {
            if (Objects.equals(METHOD_GET, exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_OK, null);
            } else {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, null);
            }
        }));

        httpServer.createContext("/v0/entity", new ErrorHttpHandler(exchange -> {
            final Map<String, String> queryParams = parseQueryParams(exchange.getRequestURI().getQuery());
            final String id = parseId(queryParams);
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
        final URI uri = URI.create(replicaEndpoint);
        final int grpcPort = parseGrpcPort(replicaEndpoint);
        final ManagedChannel channel = channelByEndpoint.computeIfAbsent(replicaEndpoint, ignored ->
            ManagedChannelBuilder.forAddress(uri.getHost(), grpcPort)
                .usePlaintext()
                .build()
        );

        final InternalRequest.Builder requestBuilder = InternalRequest.newBuilder()
            .setMethod(method)
            .setId(id);
        if (body != null) {
            requestBuilder.setBody(ByteString.copyFrom(body));
        }

        final DPInternalKvServiceGrpc.DPInternalKvServiceBlockingStub stub = DPInternalKvServiceGrpc
            .newBlockingStub(channel)
            .withDeadlineAfter(PROXY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        final InternalResponse response;
        try {
            response = stub.handle(requestBuilder.build());
        } catch (StatusRuntimeException e) {
            return new OperationResult(STATUS_GATEWAY_TIMEOUT, null);
        }
        final byte[] responseBody = response.getBody().isEmpty() ? null : response.getBody().toByteArray();
        return new OperationResult(response.getStatusCode(), responseBody);
    }

    private static boolean isSuccessfulWriteStatus(int statusCode, String method) {
        return (METHOD_PUT.equals(method) && statusCode == STATUS_CREATED)
            || (METHOD_DELETE.equals(method) && statusCode == STATUS_ACCEPTED);
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

    private static int parseGrpcPort(String endpoint) {
        final URI uri = URI.create(endpoint);
        final Map<String, String> queryParams = parseQueryParams(uri.getQuery());
        final String grpcPort = queryParams.get(GRPC_PORT_PARAM);
        if (grpcPort == null || grpcPort.isBlank()) {
            throw new IllegalArgumentException("grpcPort is not configured: " + endpoint);
        }
        return Integer.parseInt(grpcPort);
    }

    private static Map<String, String> parseQueryParams(String query) {
        final Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }
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
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start grpc server", e);
        }
        httpServer.start();
        log.info("Node started. endpoint={}", localEndpoint);
    }

    @Override
    public void stop() {
        httpServer.stop(0);
        grpcServer.shutdownNow();
        for (ManagedChannel channel : channelByEndpoint.values()) {
            channel.shutdownNow();
        }
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

    private final class InternalGrpcService extends DPInternalKvServiceGrpc.DPInternalKvServiceImplBase {
        @Override
        public void handle(InternalRequest request, StreamObserver<InternalResponse> responseObserver) {
            final byte[] body = request.getBody().isEmpty() ? null : request.getBody().toByteArray();
            final OperationResult result = executeLocally(request.getMethod(), request.getId(), body);
            final InternalResponse.Builder responseBuilder = InternalResponse.newBuilder()
                .setStatusCode(result.statusCode);
            if (result.body != null) {
                responseBuilder.setBody(ByteString.copyFrom(result.body));
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
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
