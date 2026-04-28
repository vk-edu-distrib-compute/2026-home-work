package company.vk.edu.distrib.compute.usl.sharding;

import java.util.List;

public interface ShardingStrategy {
    String resolveOwner(String key);

    List<String> endpoints();
}
