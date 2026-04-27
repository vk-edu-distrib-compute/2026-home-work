package company.vk.edu.distrib.compute.marinchanka;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.marinchanka.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MarinchankaKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(MarinchankaKVService.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String PARAM_ID = "id";
    private static final String CONTENT_TYPE_VALUE = "application/octet-stream";

    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int INTERNAL_ERROR = 500;
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int SERVICE_UNAVAILABLE = 503;

    private final int port;
    private final int grpcPort;
    private final Dao<byte[]> dao;
    private final ShardingRouter router;
    private final Map<String, Integer> grpcPorts = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> grpcChannels = new ConcurrentHashMap<>();
    private HttpServer server;
    private Server grpcServer;
    private boolean running;

    public MarinchankaKVService(int port, Dao<byte[]> dao) {
        this(port, port + 1000, dao, null);
    }

    public MarinchankaKVService(int port, Dao<byte[]> dao, ShardingRouter router) {
        this(port, port + 1000, dao, router);
    }

    public MarinchankaKVService(int port, int grpcPort, Dao<byte[]> dao, ShardingRouter router) {
        this.port = port;
        this.grpcPort = grpcPort;
        this.dao = dao;
        this.router = router;
    }

    public void setGrpcPorts(Map<String, Integer> ports) {
        this.grpcPorts.clear();
        this.grpcPorts.putAll(ports);
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(null);
            server.start();

            grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new KVInternalServiceImpl())
                    .build()
                    .start();

            running = true;
            log.info("KVService started on port {} (gRPC {})", port, grpcPort);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        if (server != null) {
            server.stop(0);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
        grpcChannels.values().forEach(channel -> {
            channel.shutdown();
        });
        grpcChannels.clear();
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error closing DAO", e);
        }
    }

    public void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error closing DAO", e);
        }
    }

    private final class KVInternalServiceImpl extends KVInternalServiceGrpc.KVInternalServiceImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            try {
                byte[] value = dao.get(request.getKey());
                responseObserver.onNext(GetResponse.newBuilder()
                        .setValue(ByteString.copyFrom(value))
                        .setFound(true)
                        .build());
            } catch (NoSuchElementException e) {
                responseObserver.onNext(GetResponse.newBuilder().setFound(false).build());
            } catch (Exception e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            try {
                dao.upsert(request.getKey(), request.getValue().toByteArray());
                responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
            } catch (Exception e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            try {
                dao.delete(request.getKey());
                responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
            } catch (Exception e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onCompleted();
        }
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                return;
            }

            if (running) {
                exchange.sendResponseHeaders(STATUS_OK, -1);
            } else {
                exchange.sendResponseHeaders(SERVICE_UNAVAILABLE, -1);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String id = extractId(exchange.getRequestURI().getQuery());

            if (id == null) {
                sendError(exchange, BAD_REQUEST, "Missing id parameter");
                return;
            }

            if (shouldProxy(id)) {
                proxyViaGrpc(exchange, router.getNode(id), id);
                return;
            }

            handleLocalRequest(exchange, method, id);
        }

        private boolean shouldProxy(String id) {
            if (router == null) {
                return false;
            }
            ClusterNode responsibleNode = router.getNode(id);
            return responsibleNode.port() != port;
        }

        private void proxyViaGrpc(HttpExchange exchange, ClusterNode targetNode, String id) throws IOException {
            int grpcPort = grpcPorts.getOrDefault(targetNode.getEndpoint(), targetNode.port() + 1000);

            ManagedChannel channel = grpcChannels.computeIfAbsent(targetNode.getEndpoint(),
                    key -> ManagedChannelBuilder.forAddress("localhost", grpcPort)
                            .usePlaintext()
                            .build());

            try {
                KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                        KVInternalServiceGrpc.newBlockingStub(channel);
                String method = exchange.getRequestMethod();

                if (METHOD_GET.equals(method)) {
                    GetRequest getReq = GetRequest.newBuilder().setKey(id).build();
                    GetResponse getResp = stub.get(getReq);
                    if (getResp.getFound()) {
                        byte[] data = getResp.getValue().toByteArray();
                        exchange.sendResponseHeaders(STATUS_OK, data.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(data);
                        }
                    } else {
                        sendError(exchange, NOT_FOUND, "Key not found");
                    }
                } else if (METHOD_PUT.equals(method)) {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    PutRequest putReq = PutRequest.newBuilder()
                            .setKey(id)
                            .setValue(ByteString.copyFrom(body))
                            .build();
                    stub.put(putReq);
                    exchange.sendResponseHeaders(STATUS_CREATED, -1);
                } else if (METHOD_DELETE.equals(method)) {
                    DeleteRequest delReq = DeleteRequest.newBuilder().setKey(id).build();
                    stub.delete(delReq);
                    exchange.sendResponseHeaders(STATUS_ACCEPTED, -1);
                } else {
                    exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                }
            } catch (Exception e) {
                log.error("gRPC proxy error for key {} to {}", id, targetNode, e);
                sendError(exchange, INTERNAL_ERROR, "Proxy error: " + e.getMessage());
            }
        }

        private void handleLocalRequest(HttpExchange exchange, String method, String id) throws IOException {
            try {
                if (METHOD_GET.equals(method)) {
                    handleGet(exchange, id);
                } else if (METHOD_PUT.equals(method)) {
                    handlePut(exchange, id);
                } else if (METHOD_DELETE.equals(method)) {
                    handleDelete(exchange, id);
                } else {
                    exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, -1);
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, BAD_REQUEST, e.getMessage());
            } catch (NoSuchElementException e) {
                sendError(exchange, NOT_FOUND, e.getMessage());
            } catch (IOException e) {
                log.error("Internal server error", e);
                sendError(exchange, INTERNAL_ERROR, "Internal server error");
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_VALUE);
            exchange.sendResponseHeaders(STATUS_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(STATUS_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            exchange.sendResponseHeaders(STATUS_ACCEPTED, -1);
        }

        private String extractId(String query) {
            if (query == null) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && PARAM_ID.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void sendError(HttpExchange exchange, int code, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(code, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }

    public Dao<byte[]> getDao() {
        return dao;
    }
}
