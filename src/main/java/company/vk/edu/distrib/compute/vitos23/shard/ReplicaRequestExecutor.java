package company.vk.edu.distrib.compute.vitos23.shard;

import company.vk.edu.distrib.compute.vitos23.internal.InternalGrpcKVClient;
import company.vk.edu.distrib.compute.vitos23.util.IOSupplier;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class ReplicaRequestExecutor implements AutoCloseable {

    private final ShardResolver shardResolver;
    private final int replicationFactor;
    private final String localEndpoint;
    private final ConcurrentMap<String, InternalGrpcKVClient> internalClientByEndpoint = new ConcurrentHashMap<>();

    public ReplicaRequestExecutor(ShardResolver shardResolver, int replicationFactor, String localEndpoint) {
        this.shardResolver = shardResolver;
        this.replicationFactor = replicationFactor;
        this.localEndpoint = localEndpoint;
    }

    @Override
    public void close() throws Exception {
        for (var client : internalClientByEndpoint.values()) {
            client.stop();
        }
    }

    /// Execute action on all replicas associated with the key.
    /// This method only initiates requests without waiting for completion.
    public <T> List<Mono<T>> executeOnReplicas(
            String id,
            Function<InternalGrpcKVClient, Mono<T>> requestAction,
            IOSupplier<Mono<T>> localValueSupplier
    ) {
        List<ShardInfo> replicaNodes = shardResolver.resolveNodes(id, replicationFactor);
        return replicaNodes.stream()
                .map(shardInfo -> executeOnReplica(requestAction, localValueSupplier, shardInfo))
                .toList();
    }

    private <T> Mono<T> executeOnReplica(
            Function<InternalGrpcKVClient, Mono<T>> requestAction,
            IOSupplier<Mono<T>> localValueSupplier,
            ShardInfo shardInfo
    ) {
        if (localEndpoint.equals(shardInfo.httpEndpoint())) {
            try {
                return localValueSupplier.get();
            } catch (IOException e) {
                return Mono.error(e);
            }
        }
        return requestAction.apply(getClient(shardInfo));
    }

    private InternalGrpcKVClient getClient(ShardInfo shard) {
        // ConcurrentHashMap performs it atomically so the client won't be created several times
        return internalClientByEndpoint.computeIfAbsent(shard.grpcEndpoint(), InternalGrpcKVClient::new);
    }
}
