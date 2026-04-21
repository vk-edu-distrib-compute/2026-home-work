package company.vk.edu.distrib.compute.martinez1337.sharding;

import java.util.List;

public interface ShardingStrategy {
    int getResponsibleNode(String key, List<String> endpoints);

    long hash(String input);
}
