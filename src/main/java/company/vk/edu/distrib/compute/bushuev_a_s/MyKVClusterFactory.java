package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class MyKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new MyKVCluster(ports, new MyRendezvousHashing());
    }

    protected KVCluster doCreate(List<Integer> ports, MyHashingStrategy hashfunction) {
        return new MyKVCluster(ports, hashfunction);
    }

    public KVCluster create(List<Integer> ports, MyHashingStrategy hashfunction) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("Missing ports for the cluster");
        }

        return doCreate(ports, hashfunction);
    }
}
