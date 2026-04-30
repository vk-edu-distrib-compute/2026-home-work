package company.vk.edu.distrib.compute.nihuaway00.sharding;

import com.sun.net.httpserver.HttpExchange;

public class LocalShardRouter implements ShardRouter {
    private final String currentNodeEndpoint;

    public LocalShardRouter(String currentNodeEndpoint) {
        this.currentNodeEndpoint = currentNodeEndpoint;
    }

    @Override
    public void proxyRequest(HttpExchange exchange, String targetNodeEndpoint) {
        // В локальном роутере не будет вызван proxy никогда,
        // т.к. роутер всегда указывает на текущую (локальную) ноду.
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
