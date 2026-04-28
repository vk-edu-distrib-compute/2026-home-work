package company.vk.edu.distrib.compute.wolfram158;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GrpcKVServiceImpl extends GrpcKVServiceGrpc.GrpcKVServiceImplBase implements KVService {
    private final HttpServer server;
    private final Server grpcServer;
    private Router router;
    private final String endpoint;
    private final Dao<byte[]> dao;
    private static final String ENTITY_SUFFIX = "/v0/entity";
    private final ConcurrentMap<String, GrpcKVServiceGrpc.GrpcKVServiceBlockingStub> grpcStubs;

    public GrpcKVServiceImpl(final int port, final Dao<byte[]> dao) throws IOException {
        super();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        endpoint = Utils.mapToLocalhostEndpoint(port);
        grpcServer = ServerBuilder.forPort(port + Utils.GRPC_DIFF)
                .addService(this)
                .build();
        grpcStubs = new ConcurrentHashMap<>();
        this.dao = dao;
        addStatusHandler();
        addEntityHandler();
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    private GrpcKVServiceGrpc.GrpcKVServiceBlockingStub getGrpcStub(String endpoint) {
        return grpcStubs.computeIfAbsent(endpoint, addr -> {
            final ManagedChannel channel = Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create())
                    .build();
            return GrpcKVServiceGrpc.newBlockingStub(channel).withDeadline(Deadline.after(5, TimeUnit.SECONDS));
        });
    }

    @Override
    public void start() {
        server.start();
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        server.stop(0);
        grpcServer.shutdown();
    }

    @Override
    public void get(KeyRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            final byte[] value = dao.get(request.getKey());
            final GetResponse response = GetResponse.newBuilder()
                    .setValue(ByteString.copyFrom(value))
                    .setStatus(HttpURLConnection.HTTP_OK)
                    .build();
            responseObserver.onNext(response);
        } catch (NoSuchElementException e) {
            responseObserver.onNext(GetResponse.newBuilder().setStatus(HttpURLConnection.HTTP_NOT_FOUND).build());
        } catch (IOException e) {
            responseObserver.onNext(GetResponse.newBuilder().setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR).build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<EmptyResponse> responseObserver) {
        try {
            dao.upsert(request.getKey(), request.getValue().toByteArray());
            responseObserver.onNext(EmptyResponse.newBuilder().setStatus(HttpURLConnection.HTTP_CREATED).build());
        } catch (IOException e) {
            responseObserver.onNext(EmptyResponse.newBuilder()
                    .setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR).build()
            );
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void delete(KeyRequest request, StreamObserver<EmptyResponse> responseObserver) {
        try {
            dao.delete(request.getKey());
            responseObserver.onNext(EmptyResponse.newBuilder().setStatus(HttpURLConnection.HTTP_ACCEPTED).build());
        } catch (IOException e) {
            responseObserver.onNext(EmptyResponse.newBuilder()
                    .setStatus(HttpURLConnection.HTTP_INTERNAL_ERROR).build()
            );
        } finally {
            responseObserver.onCompleted();
        }
    }

    private void addStatusHandler() {
        server.createContext("/v0/status", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            }
        });
    }

    private void addEntityHandler() {
        server.createContext(ENTITY_SUFFIX, exchange -> {
            if (router != null) {
                final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
                final List<String> values = queries.get(QueryParamConstants.ID);
                if (values.isEmpty() || values.getFirst().isEmpty()) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                    return;
                }
                try {
                    final String workEndpoint = router.getNode(values.getFirst());
                    final String workGrpcEndpoint = Utils.mapToLocalhostGrpcEndpoint(Utils.extractPort(workEndpoint));
                    if (!workEndpoint.equals(endpoint)) {
                        handleProxy(exchange, workGrpcEndpoint, values.getFirst());
                        return;
                    }
                } catch (NoSuchAlgorithmException ignored) {
                    return;
                }
            }
            handleLocally(exchange);
        });
    }

    private void handleLocally(final HttpExchange exchange) throws IOException {
        switch (exchange.getRequestMethod()) {
            case HttpMethodConstants.GET: {
                handleGetEntity(exchange);
                break;
            }

            case HttpMethodConstants.PUT: {
                handlePutEntity(exchange);
                break;
            }

            case HttpMethodConstants.DELETE: {
                handleDeleteEntity(exchange);
                break;
            }

            default: {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                exchange.close();
                break;
            }
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleProxy(
            final HttpExchange exchange,
            final String workGrpcEndpoint,
            final String key
    ) throws IOException {
        final GrpcKVServiceGrpc.GrpcKVServiceBlockingStub stub = getGrpcStub(workGrpcEndpoint);

        try {
            switch (exchange.getRequestMethod()) {
                case HttpMethodConstants.GET -> {
                    final GetResponse response = stub.get(KeyRequest.newBuilder()
                            .setKey(key).build());
                    if (response.getStatus() == HttpURLConnection.HTTP_OK) {
                        final byte[] body = response.getValue().toByteArray();
                        exchange.sendResponseHeaders(response.getStatus(), body.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(body);
                        }
                    } else {
                        exchange.sendResponseHeaders(response.getStatus(), -1);
                    }
                }
                case HttpMethodConstants.PUT -> {
                    final byte[] body = exchange.getRequestBody().readAllBytes();
                    final EmptyResponse response = stub.put(PutRequest.newBuilder()
                            .setKey(key)
                            .setValue(ByteString.copyFrom(body))
                            .build());
                    exchange.sendResponseHeaders(response.getStatus(), -1);
                }
                case HttpMethodConstants.DELETE -> {
                    final EmptyResponse response = stub.delete(KeyRequest.newBuilder().setKey(key).build());
                    exchange.sendResponseHeaders(response.getStatus(), -1);
                }
                default -> exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
        } finally {
            exchange.close();
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleGetEntity(
            final HttpExchange exchange
    ) throws IOException {
        try {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            final byte[] response = dao.get(values.getFirst());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        } finally {
            exchange.close();
        }
    }

    private void handlePutEntity(
            final HttpExchange exchange
    ) throws IOException {
        try (exchange; InputStream is = exchange.getRequestBody()) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            dao.upsert(values.getFirst(), is.readAllBytes());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
        }
    }

    private void handleDeleteEntity(
            final HttpExchange exchange
    ) throws IOException {
        try (exchange) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            } else {
                dao.delete(values.getFirst());
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
            }
        }
    }
}
