package company.vk.edu.distrib.compute.glekoz.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class KVServiceClusterFactoryGK extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVServiceClusterGK(ports);
    }

}
