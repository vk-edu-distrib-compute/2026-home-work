package company.vk.edu.distrib.compute.denchika.factory.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.denchika.cluster.DenchikaKVCluster;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.DistributingAlgorithm;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.RendezvousHashing;
import company.vk.edu.distrib.compute.denchika.dao.InMemoryDao;

import java.util.List;

public class DenchikaRendezvousKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .map(p -> "http://localhost:" + p)
                .toList();
        DistributingAlgorithm hasher = new RendezvousHashing(endpoints);
        return new DenchikaKVCluster(ports, new InMemoryDao(), hasher);
    }
}
