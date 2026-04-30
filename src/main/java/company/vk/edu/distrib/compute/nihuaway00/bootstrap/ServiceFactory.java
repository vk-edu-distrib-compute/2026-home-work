package company.vk.edu.distrib.compute.nihuaway00.bootstrap;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nihuaway00.Config;
import company.vk.edu.distrib.compute.nihuaway00.NodeServer;
import company.vk.edu.distrib.compute.nihuaway00.app.InternalNodeClient;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaNode;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaSelector;
import company.vk.edu.distrib.compute.nihuaway00.cluster.DistributedShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.cluster.LocalShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.cluster.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.cluster.ShardingStrategy;
import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.InternalGrpcClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServiceFactory extends company.vk.edu.distrib.compute.KVServiceFactory {
    private final ShardingStrategy shardingStrategy;
    private final int replicaCount;
    private final Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs;

    public ServiceFactory(ShardingStrategy shardingStrategy, int replicaCount, Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs) {
        super();
        this.shardingStrategy = shardingStrategy;
        this.replicaCount = replicaCount;
        this.stubs = stubs;

    }

    public ServiceFactory() {
        this(null, Config.replicas(), Map.of());
    }

    private ShardRouter buildShardRouter(int port, Map<String, ReactorKVServiceGrpc.ReactorKVServiceStub> stubs) {

        return shardingStrategy != null
                ? new DistributedShardRouter("http://localhost:" + port, shardingStrategy, stubs)
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
        ShardRouter shardRouter = buildShardRouter(port, stubs);
        ReplicaManager replicaManager = buildReplicaManager(port, replicaCount);
        InternalNodeClient internalNodeClient = new InternalGrpcClient(stubs);
        KVCommandService commandService = new KVCommandService(replicaManager, shardRouter, internalNodeClient);
        return new NodeServer(port, commandService);
    }
}
