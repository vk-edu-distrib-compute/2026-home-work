package company.vk.edu.distrib.compute.artttnik.shard;

import java.util.List;

@FunctionalInterface
public interface ShardingStrategy {
    String resolveOwner(String key, List<String> endpoints);
}
