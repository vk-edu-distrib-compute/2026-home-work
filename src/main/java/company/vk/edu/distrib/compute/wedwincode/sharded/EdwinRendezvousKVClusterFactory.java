package company.vk.edu.distrib.compute.wedwincode.sharded;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class EdwinRendezvousKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new RendezvousKVClusterImpl(ports);
    }
}
