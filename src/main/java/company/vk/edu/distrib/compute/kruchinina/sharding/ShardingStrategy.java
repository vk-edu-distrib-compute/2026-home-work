package company.vk.edu.distrib.compute.kruchinina.sharding;

import java.util.List;

@FunctionalInterface
public interface ShardingStrategy {
    String selectNode(String key, List<String> nodes);
}
