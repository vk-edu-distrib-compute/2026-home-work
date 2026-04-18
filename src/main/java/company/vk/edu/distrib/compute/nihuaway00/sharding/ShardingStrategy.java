package company.vk.edu.distrib.compute.nihuaway00.sharding;

public interface ShardingStrategy {
    NodeInfo getResponsibleNode(String key);
}
