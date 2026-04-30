package company.vk.edu.distrib.compute.nihuaway00.cluster;

public interface ShardRouter {
    <T> T proxyRequest(String targetNodeEndpoint, ShardOperation<T> operation);

    String getResponsibleNode(String key);

    boolean isLocalNode(String endpoint);
}
