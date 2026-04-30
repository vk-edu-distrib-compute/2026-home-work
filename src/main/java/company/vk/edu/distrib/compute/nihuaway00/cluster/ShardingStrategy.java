package company.vk.edu.distrib.compute.nihuaway00.cluster;

import java.util.List;

public interface ShardingStrategy {
    ClusterNode getResponsibleNode(String key);

    void enableNode(String endpoint);

    void disableNode(String endpoint);

    List<String> getEndpoints();
}
