package company.vk.edu.distrib.compute.arseniy90.replication;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.arseniy90.model.Response;
import company.vk.edu.distrib.compute.arseniy90.stats.StatisticsAggregator;
import company.vk.edu.distrib.compute.proto.arseniy90.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcReplicaClient {
    private static final String GET_QUERY = "GET";
    private static final String PUT_QUERY = "PUT";
    private static final String DELETE_QUERY = "DELETE";

    private final HttpClient client;
    private final ExecutorService executor;
    private final String currentEndpoint;
    private final Dao<byte[]> dao;
    private final StatisticsAggregator statsAggregator;
    private final Set<Integer> disabledNodes;

    private final Map<String, GrpcKVServiceGrpc.GrpcKVServiceFutureStub> stubs = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(GrpcReplicaClient.class);

    public GrpcReplicaClient(HttpClient client, ExecutorService executor, String currentEndpoint, Dao<byte[]> dao,
           StatisticsAggregator statsAggregator, Set<Integer> disabledNodes) {
        this.client = client;
        this.executor = executor;
        this.currentEndpoint = currentEndpoint;
        this.dao = dao;
        this.statsAggregator = statsAggregator;
        this.disabledNodes = disabledNodes;
    }

    public CompletableFuture<Response> sendAsync(String targetEndpoint, String method, String id, byte[] body) {
        int targetPort = extractPort(targetEndpoint);
        if (disabledNodes.contains(targetPort)) {
            return CompletableFuture.completedFuture(new Response(HttpURLConnection.HTTP_UNAVAILABLE, null));
        }

        if (targetEndpoint.equals(currentEndpoint)) {
            // log.debug("Process local request in {}", targetEndpoint);
            return CompletableFuture.supplyAsync(() -> processLocalRequest(method, id, body), executor);
        }
        var stub = getOrCreateStub(targetEndpoint);

        switch (method) {
            case GET_QUERY -> {
                var future = stub.get(GetRequest.newBuilder().setKey(id).build());
                return createFuture(future);
            }
            case PUT_QUERY -> {
                var future = stub.upsert(UpsertRequest.newBuilder().setKey(id).setValue(
                    ByteString.copyFrom(body)).build());
                return createFuture(future);
            }
            case DELETE_QUERY -> {
                var future = stub.delete(DeleteRequest.newBuilder().setKey(id).build());
                return createFuture(future);
            }
            default -> {
               return CompletableFuture.completedFuture(new Response(HttpURLConnection.HTTP_BAD_METHOD, null));
            }
        }
    }

    public Response processLocalRequest(String method, String id, byte[] body) {
        try {
            switch (method) {
                case GET_QUERY -> {
                    statsAggregator.trackRead();
                    return new Response(HttpURLConnection.HTTP_OK, dao.get(id));
                }
                case PUT_QUERY -> {
                    statsAggregator.trackWrite(id);
                    dao.upsert(id, body);
                    return new Response(HttpURLConnection.HTTP_CREATED, null);
                }
                case DELETE_QUERY -> {
                    statsAggregator.trackWrite(id);
                    dao.delete(id);
                    return new Response(HttpURLConnection.HTTP_ACCEPTED, null);
                }
                default -> {
                    return new Response(HttpURLConnection.HTTP_BAD_METHOD, null);
                }
            }
        } catch (NoSuchElementException e) {
            return new Response(HttpURLConnection.HTTP_NOT_FOUND, null);
        } catch (Exception e) {
            return new Response(HttpURLConnection.HTTP_UNAVAILABLE, null);
        }
    }

    public void processLocalStats(HttpExchange exchange, String path) throws IOException {
        String responseBody;
        if (path.endsWith("/access")) {
            responseBody = statsAggregator.getAccessJson();
        } else {
            responseBody = statsAggregator.getKeysJson();
        }

        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    public void processRemoteStats(HttpExchange exchange, String targetEndpoint, String path) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetEndpoint + path))
                .GET()
                .timeout(Duration.ofSeconds(1))
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(resp -> new Response(resp.statusCode(), resp.body()))
            .exceptionally(e -> new Response(HttpURLConnection.HTTP_UNAVAILABLE, null))
            .thenAccept(resp -> sendStatResponse(exchange, resp.status(), resp.body()))
            .whenComplete((v, ex) -> {
            if (ex != null) {
                exchange.close();
            }
        });
    }

    private void sendStatResponse(HttpExchange exchange, int status, byte[] body) {
        try (exchange) {
            int length = (body != null && body.length > 0) ? body.length : -1;
            exchange.sendResponseHeaders(status, length);
            if (length > 0) {
                exchange.getResponseBody().write(body);
            }
        } catch (IOException e) {
            log.error("Failed to send response", e);
        }
    }

    private GrpcKVServiceGrpc.GrpcKVServiceFutureStub getOrCreateStub(String endpoint) {
        return stubs.computeIfAbsent(endpoint, e -> {
            URI uri = URI.create(e);
            int remoteGrpcPort = uri.getPort() + 1000;

            ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), remoteGrpcPort)
                    .usePlaintext()
                    .executor(executor)
                    .build();

            channels.put(e, channel);
            return GrpcKVServiceGrpc.newFutureStub(channel);
        });
    }

    private CompletableFuture<Response> createFuture(ListenableFuture<InternalResponse> grpcFuture) {
        CompletableFuture<Response> cf = new CompletableFuture<>();
        Futures.addCallback(grpcFuture, new FutureCallback<>() {
            @Override public void onSuccess(InternalResponse result) {
                cf.complete(new Response(result.getStatus(), result.getValue().toByteArray()));
            }

            @Override public void onFailure(Throwable t) {
                cf.complete(new Response(HttpURLConnection.HTTP_UNAVAILABLE, null));
            }
        }, executor);
        return cf;
    }

    private static int extractPort(String endpoint) {
        try {
            return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
        } catch (Exception e) {
            return -1;
        }
    }

    public void stop() {
        channels.values().forEach(channel -> {
            try {
                channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        });
        channels.clear();
        stubs.clear();
    }
}
