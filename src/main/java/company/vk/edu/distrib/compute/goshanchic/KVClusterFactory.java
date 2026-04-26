package company.vk.edu.distrib.compute.goshanchic;

import java.io.IOException;
import java.util.List;

@FunctionalInterface
public interface KVClusterFactory {

    KVCluster doCreate(List<Integer> ports) throws IOException;
}

