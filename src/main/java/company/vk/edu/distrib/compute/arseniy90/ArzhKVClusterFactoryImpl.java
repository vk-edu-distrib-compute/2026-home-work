package company.vk.edu.distrib.compute.arseniy90;

import java.nio.file.Path;
import java.util.List;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

public class ArzhKVClusterFactoryImpl extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        // return new KVClusterImpl(ports, Path.of("./cluster_data"), HashStrategy.RENDEZVOUS);
        return new KVClusterImpl(ports, Path.of("./cluster_data"), HashStrategy.CONSISTENT);
    }
}
