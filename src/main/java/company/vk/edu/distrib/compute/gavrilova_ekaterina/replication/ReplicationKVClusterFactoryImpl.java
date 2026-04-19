package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class ReplicationKVClusterFactoryImpl extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new ReplicationKVClusterImpl(ports);
    }

}
