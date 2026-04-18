package company.vk.edu.distrib.compute.vitos23.shard;

import java.util.List;

@FunctionalInterface
public interface ShardResolver {
    /// Returns endpoints of the `min(count, numberOfShards)` nodes that store replicas of the given key
    List<String> resolveNodes(String key, int count);
}
