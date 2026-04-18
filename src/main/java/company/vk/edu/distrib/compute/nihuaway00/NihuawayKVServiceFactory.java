package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;

import java.io.IOException;

public class NihuawayKVServiceFactory extends company.vk.edu.distrib.compute.KVServiceFactory {
    private final ShardingStrategy shardingStrategy;

    public NihuawayKVServiceFactory(ShardingStrategy shardingStrategy) {
        this.shardingStrategy = shardingStrategy;
    }


    @Override
    protected KVService doCreate(int port) throws IOException {
        return new NihuawayKVService(port, shardingStrategy);
    }
}
