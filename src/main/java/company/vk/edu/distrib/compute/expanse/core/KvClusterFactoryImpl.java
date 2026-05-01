package company.vk.edu.distrib.compute.expanse.core;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class KvClusterFactoryImpl extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVShardingClusterImpl(ports.size(), ports);
    }
}
