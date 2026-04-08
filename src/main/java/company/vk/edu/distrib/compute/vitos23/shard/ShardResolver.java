package company.vk.edu.distrib.compute.vitos23.shard;

@FunctionalInterface
public interface ShardResolver {
    /// Returns endpoint of the node that stores the given key
    String resolveNode(String key);
}
