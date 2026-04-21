package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaNode;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaSelector;
import company.vk.edu.distrib.compute.nihuaway00.sharding.DistributedShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.LocalShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;
import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

public class NihuawayKVServiceFactory extends company.vk.edu.distrib.compute.KVServiceFactory {
    private final ShardingStrategy shardingStrategy;
    private final HttpClient httpClient;
    private final int replicaCount;

    public NihuawayKVServiceFactory(ShardingStrategy shardingStrategy, HttpClient httpClient, int replicaCount) {
        super();
        this.shardingStrategy = shardingStrategy;
        this.httpClient = httpClient;
        this.replicaCount = replicaCount;
    }

    public NihuawayKVServiceFactory() {
        int replicaCountProps = Config.replicas();
        this(null, null, replicaCountProps);
    }

    private ShardRouter buildShardRouter(int port) {
        return shardingStrategy != null && httpClient != null
                ? new DistributedShardRouter("http://localhost:" + port, shardingStrategy, httpClient)
                : new LocalShardRouter("http://localhost:" + port);
    }

    private ReplicaManager buildReplicaManager(int port, int replicaCount) throws IOException {
        List<ReplicaNode> replicas = new ArrayList<>();
        for (int i = 0; i < replicaCount; i++) {
            replicas.add(new ReplicaNode(i, EntityDao.createReplica(port, i)));
        }

        ReplicaSelector replicaSelector = new ReplicaSelector();
        return new ReplicaManager(replicas, replicaSelector);
    }


    @Override
    protected KVService doCreate(int port) throws IOException {
        ShardRouter shardRouter = buildShardRouter(port);
        ReplicaManager replicaManager = buildReplicaManager(port, replicaCount);

        return new NihuawayKVService(port, shardRouter, replicaManager);
    }
}
