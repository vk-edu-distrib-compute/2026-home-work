package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class VredakonKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        if (ports != null) {
            return new VredakonKVCluster(ports);
        }
        return null;
    }
}
