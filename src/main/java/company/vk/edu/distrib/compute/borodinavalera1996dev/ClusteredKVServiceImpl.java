package company.vk.edu.distrib.compute.borodinavalera1996dev;

import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.borodinavalera1996dev.cluster.Node;
import company.vk.edu.distrib.compute.borodinavalera1996dev.hashing.HashingStrategy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ClusteredKVServiceImpl extends KVServiceImpl {

    public static final String ID = "id";
    private final HashingStrategy strategy;
    private final KVProxyClient proxyClient;
    private final String url;

    public ClusteredKVServiceImpl(int port, Path path, HashingStrategy strategy,
                                  String url, KVProxyClient proxyClient, int numberOfReplications) throws IOException {
        super(port, path, numberOfReplications);
        this.strategy = strategy;
        this.proxyClient = proxyClient;
        this.url = url;
    }

    @Override
    protected void createKVContext() {
        server.createContext(PATH_ENTITY, wrapHandler(wrapWithReplication(getKVHttpHandler())));
    }

    private HttpHandler wrapWithReplication(HttpHandler handler) {
        return exchange -> {
            Map<String, String> parms = getParms(exchange);
            String id = parms.get(ID);
            if (id == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            if (strategy == null) {
                handler.handle(exchange);
            }

            Node node = strategy.getNode(id);
            if (url.equals(node.name())) {
                handler.handle(exchange);
            } else {
                proxyClient.proxy(exchange, node);
            }
        };
    }
}
