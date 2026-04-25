package company.vk.edu.distrib.compute.marinchanka;

public interface ShardingRouter {
    void addNode(ClusterNode node);

    void removeNode(ClusterNode node);

    ClusterNode getNode(String key);
}
