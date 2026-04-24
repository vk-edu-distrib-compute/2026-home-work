package company.vk.edu.distrib.compute.vladislavguzov;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class MyKVClusterFactoty extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new MyKVCluster(ports);
    }
}
