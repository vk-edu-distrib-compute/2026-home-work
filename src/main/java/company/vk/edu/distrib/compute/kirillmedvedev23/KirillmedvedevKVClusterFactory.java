package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class KirillmedvedevKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KirillmedvedevKVCluster(ports);
    }
}
