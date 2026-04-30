package company.vk.edu.distrib.compute.nihuaway00.cluster;

import company.vk.edu.distrib.compute.nihuaway00.proto.ReactorKVServiceGrpc;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.GrpcChannelRegistry;

import java.net.URI;

public class DistributedShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;
    private final String currentNodeGrpcEndpoint;
    private final ShardingStrategy shardingStrategy;
    private final GrpcChannelRegistry channelRegistry;

    public DistributedShardRouter(
            String currentNodeEndpoint,
            ShardingStrategy strategy,
            GrpcChannelRegistry channelRegistry
    ) {
        this.currentNodeEndpoint = currentNodeEndpoint;
        this.currentNodeGrpcEndpoint = toGrpcEndpoint(currentNodeEndpoint);
        this.shardingStrategy = strategy;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public String getResponsibleNode(String key) {
        return shardingStrategy.getResponsibleNode(key).getGrpcEndpoint();
    }

    @Override
    public <T> T proxyRequest(String shardId, ShardOperation<T> operation) {
        ReactorKVServiceGrpc.ReactorKVServiceStub stub = channelRegistry.getStub(shardId);
        return operation.execute(stub);
    }

    @Override
    public boolean isLocalNode(String endpoint) {
        return currentNodeEndpoint.equals(endpoint) || currentNodeGrpcEndpoint.equals(endpoint);
    }

    private static String toGrpcEndpoint(String endpoint) {
        URI uri = URI.create(endpoint);
        return uri.getHost() + ":" + (uri.getPort() + 1);
    }
}
