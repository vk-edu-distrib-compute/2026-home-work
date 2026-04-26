package company.vk.edu.distrib.compute.denchika.factory.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.denchika.cluster.DenchikaKVCluster;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.RendezvousHashing;
import company.vk.edu.distrib.compute.denchika.dao.InMemoryDao;

import java.util.List;

public class DenchikaRendezvousKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new DenchikaKVCluster(
                ports,
                new RendezvousHashing(),
                InMemoryDao::new
        );
    }
}
