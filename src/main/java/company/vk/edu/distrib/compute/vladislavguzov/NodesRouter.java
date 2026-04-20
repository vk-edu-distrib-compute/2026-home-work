package company.vk.edu.distrib.compute.vladislavguzov;

public interface NodesRouter {

    void add(ClusterNode node, String id);

    void remove(String id);

    ClusterNode get(String key);
}
