package company.vk.edu.distrib.compute.artttnik;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.artttnik.grpc.InternalKvServiceGrpc;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyRequest;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyResponse;
import company.vk.edu.distrib.compute.artttnik.shard.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MyReplicatedKVService implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(MyReplicatedKVService.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String INTERNAL_HEADER = "X-Internal-Shard-Request";
    private static final String GRPC_PORT_PARAM = "grpcPort";

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_ERROR = 500;
    private static final int HTTP_BAD_GATEWAY = 502;

    private static final int DEFAULT_THREADS = 4;
    private static final int DEFAULT_ACK = 1;
    private static final int GRPC_PORT_OFFSET = 10_000;

    private final int servicePort;
    private final int grpcPort;
    private final MyReplicaManager replicaManager;
    private final ProxyService proxyService;

    private ExecutorService executor;
    private HttpServer server;
    private Server grpcServer;

    public MyReplicatedKVService(int port, List<Dao<byte[]>> replicaDaos) {
        this(port, defaultGrpcPort(port), replicaDaos, List.of(), new RendezvousShardingStrategy());
    }

    public MyReplicatedKVService(
            int port,
            List<Dao<byte[]>> replicaDaos,
            List<String> endpoints,
            ShardingStrategy shardingStrategy
    ) {
        this(port, defaultGrpcPort(port), replicaDaos, endpoints, shardingStrategy);
    }

    public MyReplicatedKVService(
            int port,
            int grpcPort,
            List<Dao<byte[]>> replicaDaos,
            List<String> endpoints,
            ShardingStrategy shardingStrategy
    ) {
        this.servicePort = port;
        this.grpcPort = grpcPort;
        this.replicaManager = new MyReplicaManager(replicaDaos);
        String selfEndpoint = formatEndpoint(port, grpcPort);
        this.proxyService = new ProxyService(selfEndpoint, endpoints, shardingStrategy);
    }

    public static int defaultGrpcPort(int httpPort) {
        return httpPort + GRPC_PORT_OFFSET;
    }

    public static String formatEndpoint(int httpPort, int grpcPort) {
        return "http://localhost:" + httpPort + "?" + GRPC_PORT_PARAM + "=" + grpcPort;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), servicePort),
                    0
            );

            server.createContext("/v0/status", new StatusHandler());
            server.createContext(ENTITY_PATH, new EntityHandler());

            executor = Executors.newFixedThreadPool(DEFAULT_THREADS);
            server.setExecutor(executor);

            server.start();
            grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                    .addService(new InternalGrpcService())
                    .build()
                    .start();

            if (log.isInfoEnabled()) {
                log.info("KVService started on http={} grpc={}", servicePort, grpcPort);
            }

        } catch (IOException e) {
            log.error("Failed to start server on port {}", servicePort, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        stopHttpServer();
        shutdownExecutor();
        shutdownGrpcServer();
        proxyService.close();
        closeReplicaManager();
        log.info("KVService stopped");
    }

    private void stopHttpServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void shutdownExecutor() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void shutdownGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(2, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                grpcServer.shutdownNow();
            }
        }
    }

    private void closeReplicaManager() {
        try {
            replicaManager.close();
        } catch (IOException e) {
            log.debug("Failed to close replica manager", e);
        }
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.numberOfReplicas();
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
    }

    private int parseAck(String ackValue) {
        if (ackValue == null || ackValue.isBlank()) {
            return DEFAULT_ACK;
        }
        int ack = Integer.parseInt(ackValue);
        if (ack <= 0) {
            throw new IllegalArgumentException("ack must be positive");
        }
        return ack;
    }

    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }

        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            params.put(
                    URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                    parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : ""
            );
        }
        return params;
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(
                    "GET".equalsIgnoreCase(exchange.getRequestMethod()) ? HTTP_OK : HTTP_METHOD_NOT_ALLOWED,
                    -1
            );
        }
    }

    private ProxyResult handleLocalRequest(String method, String id, int ack, byte[] body) {
        return switch (method) {
            case "GET" -> handleGetLocal(id, ack);
            case "PUT" -> handlePutLocal(id, ack, body);
            case "DELETE" -> handleDeleteLocal(id, ack);
            default -> new ProxyResult(HTTP_METHOD_NOT_ALLOWED, new byte[0]);
        };
    }

    private ProxyResult handleGetLocal(String id, int ack) {
        if (replicaManager.enabledReplicas() < ack) {
            return new ProxyResult(HTTP_INTERNAL_ERROR, new byte[0]);
        }
        var res = replicaManager.readLatest(id);
        if (res.confirmations() < ack) {
            return new ProxyResult(HTTP_INTERNAL_ERROR, new byte[0]);
        }
        if (res.latest() == null || res.latest().deleted()) {
            return new ProxyResult(HTTP_NOT_FOUND, new byte[0]);
        }
        return new ProxyResult(HTTP_OK, res.latest().value());
    }

    private ProxyResult handlePutLocal(String id, int ack, byte[] body) {
        if (replicaManager.put(id, body) < ack) {
            return new ProxyResult(HTTP_INTERNAL_ERROR, new byte[0]);
        }
        return new ProxyResult(HTTP_CREATED, new byte[0]);
    }

    private ProxyResult handleDeleteLocal(String id, int ack) {
        if (replicaManager.delete(id) < ack) {
            return new ProxyResult(HTTP_INTERNAL_ERROR, new byte[0]);
        }
        return new ProxyResult(HTTP_ACCEPTED, new byte[0]);
    }

    private final class EntityHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getRawQuery());
            String id = params.get("id");

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                return;
            }
            processEntityRequest(exchange, params, id);
        }

        private void processEntityRequest(HttpExchange exchange, Map<String, String> params, String id)
                throws IOException {
            try {
                int ack = parseAck(params.get("ack"));

                if (ack > numberOfReplicas()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }

                byte[] requestBody = "PUT".equals(exchange.getRequestMethod())
                        ? exchange.getRequestBody().readAllBytes()
                        : new byte[0];

                ProxyResult result;
                if (proxyService.shouldProxy(exchange, id)) {
                    result = proxyService.proxy(exchange.getRequestMethod(), id, params.get("ack"), requestBody);
                } else {
                    result = handleLocalRequest(exchange.getRequestMethod(), id, ack, requestBody);
                }

                writeHttpResponse(exchange, result);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
            } catch (StatusRuntimeException e) {
                exchange.sendResponseHeaders(HTTP_BAD_GATEWAY, -1);
            } catch (Exception e) {
                exchange.sendResponseHeaders(HTTP_INTERNAL_ERROR, -1);
            }
        }

        private void writeHttpResponse(HttpExchange exchange, ProxyResult result) throws IOException {
            if (result.body().length == 0) {
                exchange.sendResponseHeaders(result.statusCode(), -1);
                return;
            }

            exchange.sendResponseHeaders(result.statusCode(), result.body().length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(result.body());
            }
        }
    }

    private final class InternalGrpcService extends InternalKvServiceGrpc.InternalKvServiceImplBase {

        @Override
        public void proxy(ProxyRequest request, StreamObserver<ProxyResponse> responseObserver) {
            try {
                int ack = request.getHasAck() ? request.getAck() : DEFAULT_ACK;
                ProxyResult result = handleLocalRequest(
                        request.getMethod(),
                        request.getId(),
                        ack,
                        request.getBody().toByteArray()
                );

                ProxyResponse response = ProxyResponse.newBuilder()
                        .setStatusCode(result.statusCode())
                        .setBody(ByteString.copyFrom(result.body()))
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("gRPC proxy handler error", e);
                responseObserver.onNext(ProxyResponse.newBuilder().setStatusCode(HTTP_INTERNAL_ERROR).build());
                responseObserver.onCompleted();
            }
        }
    }

    private static class ProxyService {

        private final String self;
        private final List<String> endpoints;
        private final ShardingStrategy strategy;
        private final Map<String, ManagedChannel> channels;

        ProxyService(String self, List<String> endpoints, ShardingStrategy strategy) {
            this.self = self;
            this.endpoints = endpoints;
            this.strategy = strategy;
            this.channels = new ConcurrentHashMap<>();
        }

        boolean shouldProxy(HttpExchange exchange, String id) {
            return !endpoints.isEmpty()
                    && !"true".equals(exchange.getRequestHeaders().getFirst(INTERNAL_HEADER))
                    && !self.equals(strategy.resolveOwner(id, endpoints));
        }

        ProxyResult proxy(String method, String id, String rawAck, byte[] body) {
            String owner = strategy.resolveOwner(id, endpoints);
            EndpointInfo endpointInfo = EndpointInfo.from(owner);
            ManagedChannel channel = channels.computeIfAbsent(
                    endpointInfo.channelKey(),
                    key -> ManagedChannelBuilder
                            .forAddress(endpointInfo.host(), endpointInfo.grpcPort())
                            .usePlaintext()
                            .build()
            );

            ProxyRequest.Builder requestBuilder = ProxyRequest.newBuilder()
                    .setMethod(method)
                    .setId(id)
                    .setBody(ByteString.copyFrom(body));

            if (rawAck != null && !rawAck.isBlank()) {
                requestBuilder.setHasAck(true).setAck(Integer.parseInt(rawAck));
            }

            ProxyResponse response = InternalKvServiceGrpc.newBlockingStub(channel).proxy(requestBuilder.build());
            return new ProxyResult(response.getStatusCode(), response.getBody().toByteArray());
        }

        void close() {
            for (ManagedChannel channel : channels.values()) {
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        }

        private record EndpointInfo(String host, int grpcPort) {

            private static EndpointInfo from(String endpoint) {
                URI uri = URI.create(endpoint);
                String host = uri.getHost();
                if (host == null || host.isBlank()) {
                    throw new IllegalArgumentException("Endpoint host is empty: " + endpoint);
                }
                int grpcPort = extractGrpcPort(uri);
                return new EndpointInfo(host, grpcPort);
            }

            private static int extractGrpcPort(URI uri) {
                Map<String, String> queryParams = parseQueryParams(uri.getRawQuery());
                String grpcPort = queryParams.get(GRPC_PORT_PARAM);
                if (grpcPort == null || grpcPort.isBlank()) {
                    return defaultGrpcPort(uri.getPort());
                }
                return Integer.parseInt(grpcPort);
            }

            private String channelKey() {
                return host + ":" + grpcPort;
            }
        }
    }

    private record ProxyResult(int statusCode, byte[] body) {
    }
}
