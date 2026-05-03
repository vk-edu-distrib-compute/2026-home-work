package company.vk.edu.distrib.compute.vitos23.shard;

import company.vk.edu.distrib.compute.vitos23.util.IOFunction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicaRequestExecutor {

    private final ShardResolver shardResolver;
    private final int replicationFactor;
    private final ExecutorService executor;

    public ReplicaRequestExecutor(ShardResolver shardResolver, int replicationFactor) {
        this.shardResolver = shardResolver;
        this.replicationFactor = replicationFactor;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /// Execute action on all replicas associated with the key.
    /// This method only initiates requests without waiting for completion.
    public <T> List<CompletableFuture<ReplicaResult<T>>> executeOnReplicas(
            String id,
            IOFunction<String, ReplicaResult<T>> requestAction
    ) {
        List<String> replicaNodes = shardResolver.resolveNodes(id, replicationFactor);
        return replicaNodes.stream()
                .map(endpoint -> CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return requestAction.apply(endpoint);
                            } catch (Exception e) {
                                return ReplicaResult.<T>failed();
                            }
                        },
                        executor
                ))
                .toList();
    }
}
