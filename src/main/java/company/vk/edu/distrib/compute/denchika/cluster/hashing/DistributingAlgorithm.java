package company.vk.edu.distrib.compute.denchika.cluster.hashing;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface DistributingAlgorithm {
    String selectNode(String key);
}
