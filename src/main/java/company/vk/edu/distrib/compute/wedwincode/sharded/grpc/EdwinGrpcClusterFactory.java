package company.vk.edu.distrib.compute.wedwincode.sharded.grpc;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class EdwinGrpcClusterFactory extends KVClusterFactory {

    private static final int GRPC_PORT_SHIFT = 1010;

    @Override
    protected KVCluster doCreate(List<Integer> httpPorts) {
        List<Integer> grpcPorts = httpPorts.stream().map(p -> p + GRPC_PORT_SHIFT).toList();
        return new GrpcKVClusterImpl(httpPorts, grpcPorts);
    }
}
