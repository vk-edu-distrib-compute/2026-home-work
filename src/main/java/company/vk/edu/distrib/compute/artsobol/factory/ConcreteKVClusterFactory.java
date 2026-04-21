package company.vk.edu.distrib.compute.artsobol.factory;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.artsobol.impl.KVClusterImpl;

import java.util.List;

public class ConcreteKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVClusterImpl(ports);
    }
}
