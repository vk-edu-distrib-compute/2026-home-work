package company.vk.edu.distrib.compute.vitos23.shard;

import java.util.Comparator;
import java.util.List;

import static company.vk.edu.distrib.compute.vitos23.util.HashUtils.md5Hash;

public class RendezvousShardResolver implements ShardResolver {
    private final List<String> shards;

    public RendezvousShardResolver(List<String> shards) {
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("At least one shard expected");
        }
        this.shards = shards;
    }

    @Override
    public List<String> resolveNodes(String key, int count) {
        if (count == 1) {
            // Optimization to achieve linear performance
            return List.of(resolveNode(key));
        }
        return shards.stream()
                .sorted(Comparator.comparing(shard -> md5Hash(shard + key)).reversed())
                .limit(count)
                .toList();
    }

    private String resolveNode(String key) {
        return shards.stream().max(Comparator.comparing(shard -> md5Hash(shard + key))).orElseThrow();
    }
}
