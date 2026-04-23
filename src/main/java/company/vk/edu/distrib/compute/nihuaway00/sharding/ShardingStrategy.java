package company.vk.edu.distrib.compute.nihuaway00.sharding;

import java.util.List;

public interface ShardingStrategy {
    NodeInfo getResponsibleNode(String key);

    void enableNode(String endpoint);

    void disableNode(String endpoint);

    List<String> getEndpoints();
}
