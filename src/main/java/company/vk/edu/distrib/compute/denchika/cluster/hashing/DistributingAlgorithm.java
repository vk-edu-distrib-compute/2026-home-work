package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.util.List;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface DistributingAlgorithm {
    String selectNode(String key, List<String> nodes);
}
