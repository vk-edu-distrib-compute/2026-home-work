package company.vk.edu.distrib.compute.nihuaway00.cluster;

public class LocalShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;

    public LocalShardRouter(String currentNodeEndpoint) {
        this.currentNodeEndpoint = currentNodeEndpoint;
    }

    @Override
    public <T> T proxyRequest(String targetNodeEndpoint, ShardOperation<T> operation) {
        return null;
    }

    @Override
    public String getResponsibleNode(String key) {
        return currentNodeEndpoint;
    }

    @Override
    public boolean isLocalNode(String endpoint) {
        return true;
    }
}
