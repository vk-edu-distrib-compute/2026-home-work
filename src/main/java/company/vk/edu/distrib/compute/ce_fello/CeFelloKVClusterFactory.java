package company.vk.edu.distrib.compute.ce_fello;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.nio.file.Path;
import java.util.List;

public class CeFelloKVClusterFactory extends KVClusterFactory {
    private static final Path STORAGE_ROOT = Path.of(".data", "ce_fello", "cluster");

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new CeFelloKVCluster(ports, STORAGE_ROOT, CeFelloDistributionMode.fromSystemProperty());
    }
}
