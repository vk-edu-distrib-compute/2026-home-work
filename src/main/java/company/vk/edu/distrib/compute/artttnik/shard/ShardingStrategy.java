package company.vk.edu.distrib.compute.artttnik.shard;

import java.util.List;

public interface ShardingStrategy {
    String resolveOwner(String key, List<String> endpoints);
}
