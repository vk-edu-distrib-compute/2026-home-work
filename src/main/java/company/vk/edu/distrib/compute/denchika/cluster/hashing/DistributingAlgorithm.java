package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.util.List;

public interface DistributingAlgorithm {
    String selectNode(String key, List<String> nodes);
}
