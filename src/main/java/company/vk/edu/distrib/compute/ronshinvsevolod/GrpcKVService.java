package company.vk.edu.distrib.compute.ronshinvsevolod;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.ReactorRonshinKVServiceGrpc;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinDeleteRequest;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinDeleteResponse;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinGetRequest;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinGetResponse;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinPutRequest;
import company.vk.edu.distrib.compute.ronshinvsevolod.grpc.RonshinPutResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class GrpcKVService
        extends ReactorRonshinKVServiceGrpc.RonshinKVServiceImplBase
        implements KVService {

    private static final String ID_PARAM = "id";
    private static final int EXPECTED_PARTS = 2;
    private static final int HEADER_SIZE = 9;
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 3;

    private final int httpPort;
    private final int grpcPort;
    private final Dao<byte[]> dao;
    private final List<String> peerGrpcAddresses;
    private final ReentrantLock timestampLock = new ReentrantLock();
    private long lastTimestamp = System.currentTimeMillis();

    private HttpServer httpServer;
    private Server grpcServer;

    public GrpcKVService(int httpPort, int grpcPort,
                         Dao<byte[]> dao,
                         List<String> peerGrpcAddresses) {
        super();
        this.httpPort = httpPort;
        this.grpcPort = grpcPort;
        this.dao = dao;
        this.peerGrpcAddresses = List.copyOf(peerGrpcAddresses);
    }

    @Override
    public void start() {
        try {
            grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(this)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .build()
                    .start();
            httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
            httpServer.createContext("/v0/status", new StatusHandler());
            httpServer.createContext("/v0/entity", new EntityHandler());
            httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            httpServer.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start GrpcKVService", e);
        }
    }

    @Override
    public void stop() {
        httpServer.stop(3);
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
                grpcServer.awaitTermination(1, TimeUnit.SECONDS);
        }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /*
    @Override
    public CompletableFuture<Void> awaitTermination() {
        return CompletableFuture.runAsync(() -> {
            try {
                grpcServer.awaitTermination();
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
    }
     */

    private static RonshinGetResponse buildGetResponse(byte[] raw) {
        if (raw == null || raw.length < HEADER_SIZE) {
            return RonshinGetResponse.newBuilder().setFound(false).build();
        }
        ByteBuffer buf = ByteBuffer.wrap(raw);
        long ts = buf.getLong();
        boolean deleted = buf.get() == 1;
        byte[] data = new byte[raw.length - HEADER_SIZE];
        buf.get(data);
        return RonshinGetResponse.newBuilder()
                .setFound(true)
                .setTimestamp(ts)
                .setDeleted(deleted)
                .setValue(ByteString.copyFrom(data))
                .build();
    }

    @Override
    public Mono<RonshinPutResponse> put(RonshinPutRequest request) {
        return Mono.fromCallable(() -> {
            byte[] payload = serializePut(
                    request.getTimestamp(),
                    request.getValue().toByteArray());
            dao.upsert(request.getKey(), payload);
            return RonshinPutResponse.newBuilder().setSuccess(true).build();
        }).onErrorReturn(RonshinPutResponse.newBuilder().setSuccess(false).build());
    }

    @Override
    public Mono<RonshinGetResponse> get(RonshinGetRequest request) {
        return Mono.fromCallable(() -> {
            try {
                byte[] raw = dao.get(request.getKey());
                return buildGetResponse(raw);
            } catch (NoSuchElementException e) {
                return RonshinGetResponse.newBuilder().setFound(false).build();
            }
        });
    }

    @Override
    public Mono<RonshinDeleteResponse> delete(RonshinDeleteRequest request) {
        return Mono.fromCallable(() -> {
            dao.upsert(request.getKey(), serializeDelete(request.getTimestamp()));
            return RonshinDeleteResponse.newBuilder().setSuccess(true).build();
        }).onErrorReturn(RonshinDeleteResponse.newBuilder().setSuccess(false).build());
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if (METHOD_GET.equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(HTTP_OK, -1);
                } else {
                    exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
                }
            } finally {
                exchange.close();
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String query = exchange.getRequestURI().getQuery();
                String id = extractParam(query, ID_PARAM);
                if (id == null || id.isEmpty()) {
                    exchange.sendResponseHeaders(HTTP_BAD_REQUEST, -1);
                    return;
                }
                switch (exchange.getRequestMethod()) {
                    case METHOD_GET -> handleGet(exchange, id);
                    case METHOD_PUT -> handlePut(exchange, id);
                    case METHOD_DELETE -> handleDelete(exchange, id);
                    default -> exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
                }
            } finally {
                exchange.close();
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] best = readLocal(id);
            for (String addr : peerGrpcAddresses) {
                best = mergeWithPeer(best, id, addr);
            }
            if (best == null || isDeleted(best)) {
                exchange.sendResponseHeaders(HTTP_NOT_FOUND, -1);
                return;
            }
            byte[] data = dataOf(best);
            exchange.sendResponseHeaders(HTTP_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] body;
            try (InputStream is = exchange.getRequestBody()) {
                body = is.readAllBytes();
            }
            long ts = nextTimestamp();
            dao.upsert(id, serializePut(ts, body));
            propagatePut(id, ts, body);
            exchange.sendResponseHeaders(HTTP_CREATED, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            long ts = nextTimestamp();
            dao.upsert(id, serializeDelete(ts));
            propagateDelete(id, ts);
            exchange.sendResponseHeaders(HTTP_ACCEPTED, -1);
        }
    }

    private byte[] readLocal(String id) {
        try {
            return dao.get(id);
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isDeleted(byte[] raw) {
        return raw.length >= HEADER_SIZE && raw[Long.BYTES] == 1;
    }

    private static byte[] dataOf(byte[] raw) {
        if (raw.length <= HEADER_SIZE) {
            return new byte[0];
        }
        byte[] data = new byte[raw.length - HEADER_SIZE];
        System.arraycopy(raw, HEADER_SIZE, data, 0, data.length);
        return data;
    }

    private byte[] mergeWithPeer(byte[] current, String id, String addr) {
        try {
            ManagedChannel ch = ManagedChannelBuilder
                    .forTarget(addr)
                    .usePlaintext()
                    .build();
            try {
                ReactorRonshinKVServiceGrpc.ReactorRonshinKVServiceStub stub =
                        ReactorRonshinKVServiceGrpc.newReactorStub(ch);
                RonshinGetResponse resp = stub.get(
                        RonshinGetRequest.newBuilder().setKey(id).build()).block();
                if (resp != null && resp.getFound()) {
                    byte[] remote = buildRaw(resp);
                    if (current == null || timestampOf(remote) > timestampOf(current)) {
                        return remote;
                    }
                }
            } finally {
                ch.shutdown();
            }
        } catch (Exception ignored) {
            // the node is unavailable
        }
        return current;
    }

    private static byte[] buildRaw(RonshinGetResponse resp) {
        return serializePut(resp.getTimestamp(), resp.getValue().toByteArray());
    }

    private static long timestampOf(byte[] raw) {
        return ByteBuffer.wrap(raw, 0, Long.BYTES).getLong();
    }

    private void propagatePut(String id, long ts, byte[] body) {
        for (String addr : peerGrpcAddresses) {
            try {
                ManagedChannel ch = ManagedChannelBuilder
                        .forTarget(addr)
                        .usePlaintext()
                        .build();
                try {
                    ReactorRonshinKVServiceGrpc.newReactorStub(ch)
                            .put(RonshinPutRequest.newBuilder()
                                    .setKey(id)
                                    .setTimestamp(ts)
                                    .setValue(ByteString.copyFrom(body))
                                    .build())
                            .block();
                } finally {
                    ch.shutdown();
                }
            } catch (Exception ignored) {
                // the node is unavailable
            }
        }
    }

    private void propagateDelete(String id, long ts) {
        for (String addr : peerGrpcAddresses) {
            try {
                ManagedChannel ch = ManagedChannelBuilder
                        .forTarget(addr)
                        .usePlaintext()
                        .build();
                try {
                    ReactorRonshinKVServiceGrpc.newReactorStub(ch)
                            .delete(RonshinDeleteRequest.newBuilder()
                                    .setKey(id)
                                    .setTimestamp(ts)
                                    .build())
                            .block();
                } finally {
                    ch.shutdown();
                }
            } catch (Exception ignored) {
                // the node is unavailable
            }
        }
    }

    private long nextTimestamp() {
        timestampLock.lock();
        try {
            long now = System.currentTimeMillis();
            if (now > lastTimestamp) {
                lastTimestamp = now;
            } else {
                lastTimestamp++;
            }
            return lastTimestamp;
        } finally {
            timestampLock.unlock();
        }
    }

    private static byte[] serializePut(long ts, byte[] data) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + data.length);
        buf.putLong(ts);
        buf.put((byte) 0);
        buf.put(data);
        return buf.array();
    }

    private static byte[] serializeDelete(long ts) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
        buf.putLong(ts);
        buf.put((byte) 1);
        return buf.array();
    }

    private static String extractParam(String query, String paramName) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length == EXPECTED_PARTS && paramName.equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }
}
