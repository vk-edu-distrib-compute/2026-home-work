package company.vk.edu.distrib.compute.nihuaway00.sharding;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public interface ShardRouter {
    void proxyRequest(HttpExchange exchange, String targetNodeEndpoint) throws IOException, InterruptedException;

    String getResponsibleNode(String key);

    boolean isLocalNode(String endpoint);
}
