package company.vk.edu.distrib.compute.wedwincode.sharded.grpc;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.proto.wedwincode.DeleteRequest;
import company.vk.edu.distrib.compute.proto.wedwincode.GetRequest;
import company.vk.edu.distrib.compute.proto.wedwincode.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.wedwincode.Response;
import company.vk.edu.distrib.compute.proto.wedwincode.UpsertRequest;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.exceptions.EntityException;
import company.vk.edu.distrib.compute.wedwincode.sharded.HashStrategy;
import company.vk.edu.distrib.compute.wedwincode.sharded.ShardedKVServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GrpcShardedKVServiceImpl extends ShardedKVServiceImpl {

    private final Logger log = LoggerFactory.getLogger(GrpcShardedKVServiceImpl.class);
    private final Server grpcServer;
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs = new ConcurrentHashMap<>();

    private record GrpcData(
            String key,
            ReactorKVServiceGrpc.ReactorKVServiceStub stub,
            HttpExchange exchange
    ) {}

    public GrpcShardedKVServiceImpl(
            int httpPort,
            int grpcPort,
            Dao<DaoRecord> dao,
            HashStrategy strategy
    ) throws IOException {
        super(httpPort, dao, strategy);
        this.selfEndpoint = "http://localhost:" + httpPort + "?grpcPort=" + grpcPort;
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new GrpcInternalService(dao))
                .build();
    }

    @Override
    public void start() {
        try {
            grpcServer.start();
            if (log.isInfoEnabled()) {
                log.info("gRPC server started on port {}", grpcServer.getPort());
            }
        } catch (IOException e) {
            log.error("couldn't start gRPC server", e);
            throw new IllegalStateException("Failed to start gRPC server", e);
        }
        try {
            super.start();
        } catch (RuntimeException e) {
            grpcServer.shutdown();
            throw e;
        }
    }

    @Override
    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }

        channels.values().forEach(channel -> {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });

        super.stop();
    }

    @Override
    protected void proxyRequest(HttpExchange exchange, URI uri) throws IOException {
        try {
            Map<String, String> params = parseQuery(uri.getRawQuery());
            String key = getValueFromParams("id", params);
            int grpcPort = Integer.parseInt(getValueFromParams("grpcPort", params));

            ReactorKVServiceGrpc.ReactorKVServiceStub stub = getStub(grpcPort);

            GrpcData data = new GrpcData(key, stub, exchange);
            switch (exchange.getRequestMethod()) {
                case GET_METHOD -> handleGetEntityGrpc(data);
                case PUT_METHOD -> handlePutEntityGrpc(data);
                case DELETE_METHOD -> handleDeleteEntityGrpc(data);
                default -> handleUnsupportedMethod(exchange);
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                sendEmptyResponse(HttpURLConnection.HTTP_NOT_FOUND, exchange);
            } else {
                sendEmptyResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, exchange);
            }
        } catch (Exception e) {
            log.error("error while proxying using gRPC", e);
            sendEmptyResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, exchange);
        }
    }

    private void handleGetEntityGrpc(GrpcData data) throws IOException {
        Response response = data.stub.get(
                GetRequest.newBuilder()
                        .setKey(data.key)
                        .build()
        ).block();
        log.info("got response for GET via gRPC");

        if (response == null) {
            log.error("GET via gRPC returned null response");
            sendEmptyResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, data.exchange);
            return;
        }

        if (!response.hasValue()) {
            log.info("GET via gRPC returned no value for key {}", data.key);
            sendEmptyResponse(HttpURLConnection.HTTP_NOT_FOUND, data.exchange);
            return;
        }

        byte[] body = response.getValue().getValue().toByteArray();

        try (HttpExchange exchange = data.exchange) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private void handlePutEntityGrpc(GrpcData data) throws IOException {
        try (HttpExchange exchange = data.exchange) {
            byte[] body = exchange.getRequestBody().readAllBytes();

            data.stub.upsert(
                    UpsertRequest.newBuilder()
                            .setKey(data.key)
                            .setValue(ByteString.copyFrom(body))
                            .build()
            ).block();

            log.info("PUT via gRPC");
            sendEmptyResponse(HttpURLConnection.HTTP_CREATED, exchange);
        }
    }

    private void handleDeleteEntityGrpc(GrpcData data) throws IOException {
        data.stub.delete(
                DeleteRequest.newBuilder()
                        .setKey(data.key)
                        .build()
        ).block();
        log.info("DELETE via gRPC");
        sendEmptyResponse(HttpURLConnection.HTTP_ACCEPTED, data.exchange);
    }

    private ReactorKVServiceGrpc.ReactorKVServiceStub getStub(int grpcPort) {
        return stubs.computeIfAbsent(grpcPort, port -> {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();

            channels.put(port, channel);
            return ReactorKVServiceGrpc.newReactorStub(channel);
        });
    }

    @Override
    protected URI buildEntityUri(String endpoint, String id) {
        try {
            URI baseUri = new URI(endpoint);
            String basePath = baseUri.getPath();
            String entityPath = (basePath == null || basePath.isEmpty())
                    ? "/v0/entity"
                    : basePath + "/v0/entity";
            String query = "id=" + java.net.URLEncoder.encode(
                    id,
                    java.nio.charset.StandardCharsets.UTF_8
            );
            String existingQuery = baseUri.getQuery();
            if (existingQuery != null && !existingQuery.isEmpty()) {
                query += "&" + existingQuery;
            }
            return new URI(
                    baseUri.getScheme(),
                    baseUri.getUserInfo(),
                    baseUri.getHost(),
                    baseUri.getPort(),
                    entityPath,
                    query,
                    baseUri.getFragment()
            );
        } catch (URISyntaxException e) {
            throw new EntityException("entity URI is invalid", e);
        }
    }
}
