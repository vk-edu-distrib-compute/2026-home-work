package company.vk.edu.distrib.compute.linempy.scharding;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.linempy.KVServiceImpl;
import company.vk.edu.distrib.compute.linempy.routing.ShardingStrategy;
import company.vk.edu.distrib.compute.linempy.scharding.proxy.ProxyClient;
import company.vk.edu.distrib.compute.linempy.scharding.proxy.ProxyResponse;
import company.vk.edu.distrib.compute.linempy.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KVServiceProxyImpl extends KVServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(KVServiceProxyImpl.class);

    private final ProxyClient proxyClient;
    private final String selfEndpoint;
    private final List<String> allEndpoints;
    private final ShardingStrategy shardingStrategy;
    private final Server grpcServer;

    public KVServiceProxyImpl(int port,
                              Dao<byte[]> dao,
                              List<String> allEndpoints,
                              ShardingStrategy shardingStrategy,
                              ProxyClient proxyClient)
            throws IOException {
        super(dao, port);
        this.selfEndpoint = "http://localhost:" + port;
        this.allEndpoints = allEndpoints;
        this.shardingStrategy = shardingStrategy;
        this.proxyClient = proxyClient;

        int grpcPort = port + 100;
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new GrpcServiceImpl())
                .build();
    }

    @Override
    protected void entityHandler(HttpExchange exchange) throws IOException {
        String id = detachedId(exchange.getRequestURI().getQuery());
        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        String targetNode = shardingStrategy.route(id, allEndpoints);

        if (selfEndpoint.equals(targetNode)) {
            super.entityHandler(exchange);
        } else {
            proxyRequest(exchange, id, targetNode);
        }
    }

    private void proxyRequest(HttpExchange exchange, String id, String targetNode) throws IOException {
        String targetUrl = extractBaseUrl(targetNode);
        String method = exchange.getRequestMethod();
        CompletableFuture<ProxyResponse> future;

        try {
            switch (method) {
                case "GET" -> future = proxyClient.get(targetUrl, id);
                case "PUT" -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    future = proxyClient.put(targetUrl, id, body);
                }
                case "DELETE" -> future = proxyClient.delete(targetUrl, id);
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
            }

            ProxyResponse response = future.join();
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            if (response.body().length > 0) {
                exchange.getResponseBody().write(response.body());
            }
        } catch (Exception e) {
            log.error("Proxy request failed", e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private String extractBaseUrl(String endpoint) {
        return endpoint;
    }

    @Override
    public void start() {
        super.start();
        try {
            grpcServer.start();
            log.info("gRPC server started on port {}", grpcServer.getPort());
        } catch (IOException e) {
            log.error("Failed to start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        grpcServer.shutdown();
        proxyClient.close();
        super.stop();
    }

    private class GrpcServiceImpl extends KVServiceGrpc.KVServiceImplBase {

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            try {
                byte[] value = dao.get(request.getKey());
                GetResponse response = GetResponse.newBuilder()
                        .setFound(value != null)
                        .setValue(value != null ? ByteString.copyFrom(value) : ByteString.EMPTY)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            try {
                dao.upsert(request.getKey(), request.getValue().toByteArray());
                PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                PutResponse response = PutResponse.newBuilder().setSuccess(false).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            try {
                dao.delete(request.getKey());
                DeleteResponse response = DeleteResponse.newBuilder().setSuccess(true).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                DeleteResponse response = DeleteResponse.newBuilder().setSuccess(false).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }
    }
}