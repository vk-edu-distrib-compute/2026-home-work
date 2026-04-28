package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.mediocritas.DeleteRequest;
import company.vk.edu.distrib.compute.mediocritas.GetRequest;
import company.vk.edu.distrib.compute.mediocritas.KvServiceGrpc;
import company.vk.edu.distrib.compute.mediocritas.PutRequest;
import company.vk.edu.distrib.compute.mediocritas.cluster.Node;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class GrpcProxyClient implements ProxyClient {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ProxyConfig config;
    private final Executor callbackExecutor;

    public GrpcProxyClient(ProxyConfig config) {
        this.config = config;
        this.callbackExecutor = MoreExecutors.directExecutor();
    }

    public GrpcProxyClient() {
        this(ProxyConfig.defaultConfig());
    }

    @Override
    public CompletableFuture<ProxyResponse<byte[]>> proxyGet(Node node, String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return toCompletableFuture(stub(node).get(request))
                .thenApply(response -> response.getFound()
                        ? ProxyResponse.ok(response.getValue().toByteArray())
                        : ProxyResponse.notFound())
                .exceptionally(e -> ProxyResponse.error());
    }

    @Override
    public CompletableFuture<ProxyResponse<Void>> proxyPut(Node node, String key, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        return toCompletableFuture(stub(node).put(request))
                .thenApply(response -> response.getSuccess()
                        ? ProxyResponse.created() : ProxyResponse.<Void>serverError())
                .exceptionally(e -> ProxyResponse.error());
    }

    @Override
    public CompletableFuture<ProxyResponse<Void>> proxyDelete(Node node, String key) {
        DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
        return toCompletableFuture(stub(node).delete(request))
                .thenApply(response -> response.getSuccess()
                        ? ProxyResponse.accepted() : ProxyResponse.<Void>serverError())
                .exceptionally(e -> ProxyResponse.error());
    }

    @Override
    public void close() {
        channels.values().forEach(channel -> {
            try {
                channel.shutdown().awaitTermination(
                        config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (!channel.isTerminated()) {
                    channel.shutdownNow();
                }
            }
        });
        channels.clear();
    }

    private KvServiceGrpc.KvServiceFutureStub stub(Node node) {
        ManagedChannel channel = channels.computeIfAbsent(
                node.grpcTarget(),
                target -> ManagedChannelBuilder.forTarget(target).usePlaintext().build()
        );
        return KvServiceGrpc.newFutureStub(channel)
                .withDeadlineAfter(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> future) {
        CompletableFuture<T> result = new CompletableFuture<>();
        future.addListener(() -> {
            try {
                result.complete(future.get());
            } catch (ExecutionException e) {
                result.completeExceptionally(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.completeExceptionally(e);
            }
        }, callbackExecutor);
        return result;
    }
}
