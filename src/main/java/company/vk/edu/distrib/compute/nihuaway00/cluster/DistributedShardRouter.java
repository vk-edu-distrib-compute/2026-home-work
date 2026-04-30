package company.vk.edu.distrib.compute.nihuaway00.cluster;

import company.vk.edu.distrib.compute.nihuaway00.proto.KVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.GrpcChannelRegistry;

public class DistributedShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;
    private final String currentNodeGrpcEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final GrpcChannelRegistry channelRegistry;

    public DistributedShardRouter(
            String currentNodeEndpoint,
            String currentNodeGrpcEndpoint,
            ShardingStrategy strategy,
            GrpcChannelRegistry channelRegistry
    ) {
        this.currentNodeEndpoint = currentNodeEndpoint;
        this.currentNodeGrpcEndpoint = currentNodeGrpcEndpoint;
        this.shardingStrategy = strategy;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public String getResponsibleNode(String key) {
        return shardingStrategy.getResponsibleNode(key).getGrpcEndpoint();
    }

    @Override
    public <T> T proxyRequest(String shardId, ShardOperation<T> operation) {
        KVServiceGrpc.KVServiceBlockingStub stub = channelRegistry.getStub(shardId);
        return operation.execute(stub);
    }

    @Override
    public boolean isLocalNode(String endpoint) {
        return currentNodeEndpoint.equals(endpoint) || currentNodeGrpcEndpoint.equals(endpoint);
    }
}
