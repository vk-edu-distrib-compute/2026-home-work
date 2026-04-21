package company.vk.edu.distrib.compute.nst1610;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import java.util.List;

public class Nst1610KVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new Nst1610KVCluster(ports);
    }
}
