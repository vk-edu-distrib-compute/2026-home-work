package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class Martinez1337KVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new Martinez1337KVCluster(ports);
    }
}
