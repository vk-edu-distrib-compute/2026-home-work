package company.vk.edu.distrib.compute.vitos23.shard;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static company.vk.edu.distrib.compute.vitos23.util.HashUtils.md5Hash;

/// [ShardResolver] implementation based on consistence hashing with virtual nodes.
public class ConsistentHashingShardResolver implements ShardResolver {

    /// Default value based on
    /// [Cassandra's recommendations](https://cassandra.apache.org/doc/latest/cassandra/getting-started/production.html#tokens)
    private static final int DEFAULT_VIRTUAL_NODES = 16;

    private final SortedMap<Long, String> hashRing = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHashingShardResolver(List<String> shards) {
        this(shards, DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashingShardResolver(List<String> shards, int virtualNodes) {
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("At least one shard expected");
        }
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("Virtual nodes number must be positive");
        }
        this.virtualNodes = virtualNodes;
        for (String shard : shards) {
            addShard(shard);
        }
    }

    private void addShard(String shard) {
        for (int i = 0; i < virtualNodes; i++) {
            String virtualNodeName = shard + "#" + i;
            hashRing.put(md5Hash(virtualNodeName), shard);
        }
    }

    @Override
    public String resolveNode(String key) {
        SortedMap<Long, String> tailMap = hashRing.tailMap(md5Hash(key));
        return tailMap.isEmpty() ? hashRing.firstEntry().getValue() : tailMap.firstEntry().getValue();
    }
}
