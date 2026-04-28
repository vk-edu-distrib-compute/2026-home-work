package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vitos23.internal.InternalGrpcKVService;
import company.vk.edu.distrib.compute.vitos23.shard.ShardedKVServiceFactory;

import java.io.IOException;

public class ClusterKVServiceFactory {

    private final ShardedKVServiceFactory shardedKVServiceFactory;

    public ClusterKVServiceFactory(ShardedKVServiceFactory shardedKVServiceFactory) {
        this.shardedKVServiceFactory = shardedKVServiceFactory;
    }

    public ComposedKVService create(NodePorts ports) throws IOException {
        Dao<byte[]> dao = new WalBackedDao("storage/vitos23/shard-" + ports.httpPort());
        return ComposedKVService.of(
                shardedKVServiceFactory.create(ports.httpPort(), dao),
                new InternalGrpcKVService(ports.grpcPort(), dao)
        );
    }
}
