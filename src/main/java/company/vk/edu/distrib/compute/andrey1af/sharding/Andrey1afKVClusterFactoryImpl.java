package company.vk.edu.distrib.compute.andrey1af.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class Andrey1afKVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new Andrey1afKVClusterImpl(ports);
    }
}
