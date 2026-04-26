package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class DorogovKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new DorogovKVCluster(ports);
    }
}
