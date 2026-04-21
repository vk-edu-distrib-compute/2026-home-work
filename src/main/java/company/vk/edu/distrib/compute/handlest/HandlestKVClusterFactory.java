package company.vk.edu.distrib.compute.handlest;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.io.IOException;
import java.util.List;

public class HandlestKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        try {
            return new HandlestKVCluster(ports);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create KV cluster on ports: " + ports, e);
        }
    }
}
