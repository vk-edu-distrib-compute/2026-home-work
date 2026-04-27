package company.vk.edu.distrib.compute.golubtsov_pavel;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.proto.golubtsov_pavel.KeyRequest;
import company.vk.edu.distrib.compute.proto.golubtsov_pavel.PGServiceGrpc;
import company.vk.edu.distrib.compute.proto.golubtsov_pavel.Response;
import company.vk.edu.distrib.compute.proto.golubtsov_pavel.UpsertRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PGInMemoryKVService implements KVService {
    public static final String ID_PARAM_PREFIX = "id=";
    static final String METHOD_GET = "GET";
    static final String METHOD_PUT = "PUT";
    static final String METHOD_DELETE = "DELETE";

    private static final Logger log = LoggerFactory.getLogger(PGInMemoryKVService.class);
    private static final String GRPC_PORT_PARAM = "grpcPort";
    private static final int REQUEST_TIMEOUT_SECONDS = 2;
    private static final int TERMINATION_TIMEOUT_MILLIS = 500;

    private final Dao<byte[]> dao;
    private final int httpPort;
    private final int grpcPort;
    private final String selfEndpoint;
    private final ShardResolver shardResolver;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, PGServiceGrpc.PGServiceBlockingStub> clients = new ConcurrentHashMap<>();
    private HttpServer httpServer;
    private io.grpc.Server grpcServer;

    public PGInMemoryKVService(int port,
                               int grpcPort,
                               Dao<byte[]> dao,
                               String selfEndpoint,
                               List<String> clusterEndpoints)
            throws IOException {
        this.httpPort = port;
        this.grpcPort = grpcPort;
        this.dao = dao;
        this.selfEndpoint = selfEndpoint;
        this.shardResolver = new ShardResolver(clusterEndpoints);
    }

    private void initServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/v0/status", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            if (Objects.equals(METHOD_GET, method)) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
        }));

        httpServer.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            final var id = parseId(http.getRequestURI().getQuery());
            handleEntityRequest(http, method, id);
        }));

        httpServer.createContext("/", new ErrorHttpHandler(this::handleCompatibilityRequest));
        grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                .addService(new GrpcEntityService())
                .build();
    }

    private void handleCompatibilityRequest(HttpExchange http) throws IOException {
        String query = http.getRequestURI().getRawQuery();
        if (query == null || !query.contains("/v0/entity?")) {
            http.sendResponseHeaders(404, 0);
            return;
        }
        String entityQuery = query.substring(query.indexOf("/v0/entity?") + "/v0/entity?".length());
        String id = parseId(entityQuery);
        handleEntityRequest(http, http.getRequestMethod(), id);
    }

    private void handleEntityRequest(HttpExchange http, String method, String id) throws IOException {
        String owner = shardResolver.resolve(id);
        if (owner.equals(selfEndpoint)) {
            Response response = handleLocalEntityRequest(method, id, http.getRequestBody().readAllBytes());
            writeResponse(http, response);
        } else {
            Response response = proxyRequest(method, owner, id, http.getRequestBody().readAllBytes());
            writeResponse(http, response);
        }
    }

    private static String parseId(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("bad query");
        }
        for (String param : query.split("&")) {
            if (param.startsWith(ID_PARAM_PREFIX)) {
                String id = URLDecoder.decode(param.substring(ID_PARAM_PREFIX.length()), StandardCharsets.UTF_8);
                if (id.isBlank()) {
                    throw new IllegalArgumentException("empty id");
                }
                return id;
            }
        }
        throw new IllegalArgumentException("bad query");
    }

    private Response handleLocalEntityRequest(String method, String id, byte[] body) throws IOException {
        if (Objects.equals(METHOD_GET, method)) {
            final var value = dao.get(id);
            return response(200, value);
        } else if (Objects.equals(METHOD_PUT, method)) {
            dao.upsert(id, body);
            return response(201, new byte[0]);
        } else if (Objects.equals(METHOD_DELETE, method)) {
            dao.delete(id);
            return response(202, new byte[0]);
        }
        return response(405, new byte[0]);
    }

    private Response proxyRequest(String method, String owner, String id, byte[] body) {
        PGServiceGrpc.PGServiceBlockingStub client = client(owner)
                .withDeadlineAfter(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (Objects.equals(METHOD_GET, method)) {
            return client.get(KeyRequest.newBuilder().setKey(id).build());
        } else if (Objects.equals(METHOD_PUT, method)) {
            return client.upsert(UpsertRequest.newBuilder()
                    .setKey(id)
                    .setValue(ByteString.copyFrom(body))
                    .build());
        } else if (Objects.equals(METHOD_DELETE, method)) {
            return client.delete(KeyRequest.newBuilder().setKey(id).build());
        }
        return response(405, new byte[0]);
    }

    private PGServiceGrpc.PGServiceBlockingStub client(String endpoint) {
        return clients.computeIfAbsent(endpoint, ignored -> {
            Endpoint parsed = Endpoint.parse(endpoint);
            ManagedChannel channel = Grpc.newChannelBuilder(
                    parsed.grpcTarget(),
                    InsecureChannelCredentials.create()
            ).build();
            channels.put(endpoint, channel);
            return PGServiceGrpc.newBlockingStub(channel);
        });
    }

    private static void writeResponse(HttpExchange http, Response response) throws IOException {
        byte[] body = response.getValue().toByteArray();
        http.sendResponseHeaders(response.getStatus(), body.length);
        http.getResponseBody().write(body);
    }

    private static Response response(int status, byte[] body) {
        return Response.newBuilder()
                .setStatus(status)
                .setValue(ByteString.copyFrom(body))
                .build();
    }

    @Override
    public void start() {
        log.info("Starting...");
        try {
            initServer();
            grpcServer.start();
            httpServer.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try {
                grpcServer.awaitTermination(TERMINATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        clients.clear();
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
            try {
                channel.awaitTermination(TERMINATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
        log.info("Stopped");
    }

    private final class GrpcEntityService extends PGServiceGrpc.PGServiceImplBase {
        @Override
        public void get(KeyRequest request, StreamObserver<Response> responseObserver) {
            complete(responseObserver, () -> handleLocalEntityRequest(METHOD_GET, request.getKey(), new byte[0]));
        }

        @Override
        public void upsert(UpsertRequest request, StreamObserver<Response> responseObserver) {
            complete(responseObserver, () -> handleLocalEntityRequest(
                    METHOD_PUT,
                    request.getKey(),
                    request.getValue().toByteArray()
            ));
        }

        @Override
        public void delete(KeyRequest request, StreamObserver<Response> responseObserver) {
            complete(responseObserver, () -> handleLocalEntityRequest(METHOD_DELETE, request.getKey(), new byte[0]));
        }

        private void complete(StreamObserver<Response> responseObserver, LocalAction action) {
            try {
                responseObserver.onNext(action.perform());
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                responseObserver.onNext(response(400, new byte[0]));
                responseObserver.onCompleted();
            } catch (NoSuchElementException e) {
                responseObserver.onNext(response(404, new byte[0]));
                responseObserver.onCompleted();
            } catch (IOException e) {
                responseObserver.onNext(response(500, new byte[0]));
                responseObserver.onCompleted();
            }
        }
    }

    @FunctionalInterface
    private interface LocalAction {
        Response perform() throws IOException;
    }

    private static final class Endpoint {
        private final String host;
        private final int grpcPort;

        private Endpoint(String host, int grpcPort) {
            this.host = host;
            this.grpcPort = grpcPort;
        }

        private static Endpoint parse(String endpoint) {
            URI uri = URI.create(endpoint);
            int grpcPort = uri.getPort();
            String query = uri.getRawQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    if (param.startsWith(GRPC_PORT_PARAM + "=")) {
                        grpcPort = Integer.parseInt(param.substring((GRPC_PORT_PARAM + "=").length()));
                    }
                }
            }
            return new Endpoint(uri.getHost(), grpcPort);
        }

        private String grpcTarget() {
            return host + ":" + grpcPort;
        }
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException exp) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException exp) {
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException | RuntimeException exp) {
                exchange.sendResponseHeaders(500, 0);
            } finally {
                exchange.close();
            }
        }
    }
}
