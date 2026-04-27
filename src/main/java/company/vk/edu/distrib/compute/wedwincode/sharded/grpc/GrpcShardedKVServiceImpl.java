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

public class GrpcShardedKVServiceImpl extends ShardedKVServiceImpl {

    private final Logger log = LoggerFactory.getLogger(GrpcShardedKVServiceImpl.class);
    private final Server grpcServer;

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
            log.error("couldn't start gRPC server");
        }

        super.start();
    }

    @Override
    protected void proxyRequest(HttpExchange exchange, URI uri) throws IOException {
        ManagedChannel channel = null;

        try {
            Map<String, String> params = parseQuery(uri.getRawQuery());
            String key = getValueFromParams("id", params);
            int grpcPort = Integer.parseInt(getValueFromParams("grpcPort", params));

            channel = ManagedChannelBuilder
                    .forAddress("localhost", grpcPort)
                    .usePlaintext()
                    .build();

            ReactorKVServiceGrpc.ReactorKVServiceStub stub = ReactorKVServiceGrpc.newReactorStub(channel);

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
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private void handleGetEntityGrpc(GrpcData data) throws IOException {
        Response response = data.stub.get(
                GetRequest.newBuilder()
                        .setKey(data.key)
                        .build()
        ).block();
        log.info("got response for GET via gRPC");
        assert response != null;
        byte[] body = response.getValue().getValue().toByteArray();

        data.exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
        try (OutputStream os = data.exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void handlePutEntityGrpc(GrpcData data) throws IOException {
        byte[] body = data.exchange.getRequestBody().readAllBytes();

        data.stub.upsert(
                UpsertRequest.newBuilder()
                        .setKey(data.key)
                        .setValue(ByteString.copyFrom(body))
                        .build()
        ).block();

        log.info("PUT via gRPC");
        sendEmptyResponse(HttpURLConnection.HTTP_CREATED, data.exchange);
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

    @Override
    protected URI buildEntityUri(String endpoint, String id) {
        try {
            String[] parts = endpoint.split("\\?");
            return new URI(parts[0] + "/v0/entity?id=" + id + "&" + parts[1]);
        } catch (URISyntaxException e) {
            throw new EntityException("entity URI is invalid", e);
        }
    }
}
