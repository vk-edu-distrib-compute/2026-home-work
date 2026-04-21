package company.vk.edu.distrib.compute.proteusp;

import java.util.List;

@FunctionalInterface
public interface ShardingAlgorithm {
    String getNode(String key, List<String> endpoints);
}
