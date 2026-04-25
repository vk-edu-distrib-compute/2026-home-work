package company.vk.edu.distrib.compute.vitos23.shard;

import java.util.*;

import static company.vk.edu.distrib.compute.vitos23.util.HashUtils.md5Hash;

/// [ShardResolver] implementation based on consistence hashing with virtual nodes.
public class ConsistentHashingShardResolver implements ShardResolver {

    /// Default value based on
    /// [Cassandra's recommendations](https://cassandra.apache.org/doc/latest/cassandra/getting-started/production.html#tokens)
    private static final int DEFAULT_VIRTUAL_NODES = 16;

    private final SortedMap<Long, ShardInfo> hashRing = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHashingShardResolver(List<ShardInfo> shards) {
        this(shards, DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashingShardResolver(List<ShardInfo> shards, int virtualNodes) {
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("At least one shard expected");
        }
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("Virtual nodes number must be positive");
        }
        this.virtualNodes = virtualNodes;
        for (ShardInfo shard : shards) {
            addShard(shard);
        }
    }

    private void addShard(ShardInfo shard) {
        for (int i = 0; i < virtualNodes; i++) {
            String virtualNodeName = shard.httpEndpoint() + "#" + i;
            hashRing.put(md5Hash(virtualNodeName), shard);
        }
    }

    /// Take the first `count` nodes in the order of traversal of the ring, starting from the hash point
    @Override
    public List<ShardInfo> resolveNodes(String key, int count) {
        int replicaCount = Math.min(count, hashRing.size());

        List<ShardInfo> result = new ArrayList<>(replicaCount);
        Set<String> seenNodes = new HashSet<>();

        long hash = md5Hash(key);
        SortedMap<Long, ShardInfo> tailMap = hashRing.tailMap(hash);

        for (Map.Entry<Long, ShardInfo> entry : tailMap.entrySet()) {
            if (seenNodes.add(entry.getValue().httpEndpoint())) {
                result.add(entry.getValue());
                if (result.size() == replicaCount) {
                    return result;
                }
            }
        }
        for (Map.Entry<Long, ShardInfo> entry : hashRing.entrySet()) {
            if (seenNodes.add(entry.getValue().httpEndpoint())) {
                result.add(entry.getValue());
                if (result.size() == replicaCount) {
                    return result;
                }
            }
        }

        throw new IllegalStateException("Failed to find all replicas for the key");
    }
}
