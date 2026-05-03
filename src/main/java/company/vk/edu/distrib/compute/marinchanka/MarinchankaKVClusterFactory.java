package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class MarinchankaKVClusterFactory extends KVClusterFactory {
    private static final String DEFAULT_DATA_DIR = "./cluster_data";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new MarinchankaKVCluster(ports, DEFAULT_DATA_DIR);
    }
}
