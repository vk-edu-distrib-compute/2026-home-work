package company.vk.edu.distrib.compute.vitos23.shard;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.KVServiceImpl;

import java.io.IOException;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.getLocalEndpoint;

public class ShardedKVServiceFactory {
    private final ShardResolver shardResolver;
    private final int replicationFactor;

    public ShardedKVServiceFactory(ShardResolver shardResolver, int replicationFactor) {
        super();
        this.shardResolver = shardResolver;
        this.replicationFactor = replicationFactor;
    }

    public KVService create(int port, Dao<byte[]> dao) throws IOException {
        EntityRequestProcessor shardedEntityRequestProcessor = new ShardedEntityRequestProcessor(
                new ReplicaRequestExecutor(shardResolver, replicationFactor, getLocalEndpoint(port)),
                dao,
                replicationFactor
        );
        return new KVServiceImpl(port, shardedEntityRequestProcessor);
    }
}
