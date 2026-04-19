package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nihuaway00.sharding.DistributedShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.LocalShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;

import java.io.IOException;
import java.net.http.HttpClient;

public class NihuawayKVServiceFactory extends company.vk.edu.distrib.compute.KVServiceFactory {
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;

    public NihuawayKVServiceFactory(ShardingStrategy shardingStrategy, HttpClient httpClient) {
        super();
        this.shardingStrategy = shardingStrategy;
        this.httpClient = httpClient;
    }

    public NihuawayKVServiceFactory() {
        this(null, null);
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        ShardRouter shardRouter = shardingStrategy != null && httpClient != null
                ? new DistributedShardRouter("http://localhost:" + port, shardingStrategy, httpClient)
                : new LocalShardRouter("http://localhost:" + port);
        return new NihuawayKVService(port, shardRouter);
    }
}
