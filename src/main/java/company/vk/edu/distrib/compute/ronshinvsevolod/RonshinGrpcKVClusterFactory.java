package company.vk.edu.distrib.compute.ronshinvsevolod;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class RonshinGrpcKVClusterFactory extends KVClusterFactory {

    private static final int GRPC_PORT_OFFSET = 1000;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<GrpcKVCluster.NodeConfig> configs = ports.stream()
                .map(p -> new GrpcKVCluster.NodeConfig(p, p + GRPC_PORT_OFFSET))
                .toList();
        return new GrpcKVCluster(configs);
    }
}
