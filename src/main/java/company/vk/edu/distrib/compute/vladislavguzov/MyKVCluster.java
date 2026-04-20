package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyKVCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(MyKVCluster.class);

    private final ConsistentHashRing ring;
    private final String storageBasePath;

    private final List<Integer> portsList;
    private final Map<String, ClusterNode> mapOfNodes = new ConcurrentHashMap<>();
    private boolean clusterIsRunning = false;

    public MyKVCluster(List<Integer> portsList) {
        this.portsList = portsList;
        this.ring = new ConsistentHashRing(100);
        this.storageBasePath = "storage";
    }

    @Override
    public void start() {
        if (clusterIsRunning) return;

        log.info("Starting cluster with {} nodes on ports {}", portsList.size(), portsList);
        for(int port : this.portsList) {
            String endpoint = "localhost:" + port;
            start(endpoint);
        }
        log.info("Cluster started");
        clusterIsRunning = true;
    }

    @Override
    public void start(String endpoint) {
        if (mapOfNodes.containsKey(endpoint)) {
            log.info("Node on endpoint {} already running", endpoint);
            return;
        }
        String[] hostAndPort = endpoint.split(":");
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);
        try {
            String nodeStoragePath = storageBasePath + "/node-" + host + "-"+ port;
            Dao<byte[]> dao = new FileSystemDao(nodeStoragePath);

            HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
            installHandlers(server, port, dao);
            server.start();

            ClusterNode node = new ClusterNode(port, server, dao);
            ring.add(node, node.getUrl());
            mapOfNodes.put(endpoint, node);
            log.info("Node started on endpoint {}", endpoint);

        } catch (IOException e) {
            log.error("Failed to start node on endpoint {}", endpoint, e);
            throw new RuntimeException("Failed to start node on endpoint " + endpoint + e);
        }
    }

    @Override
    public void stop() {
        if (!clusterIsRunning) return;

        log.info("Stopping cluster...");
        for (String endpoint : mapOfNodes.keySet()) {
            stop(endpoint);
        }
        mapOfNodes.clear();
        log.info("Cluster stopped");
    }

    @Override
    public void stop(String endpoint) {
        ClusterNode node = mapOfNodes.get(endpoint);
        node.server().stop(1);
        try {
            node.dao().close();
        } catch (IOException e) {
            log.info("Error closing DAO for endpoint {}", endpoint, e);
        }
        log.info("Node stopped on endpoint {}", endpoint);
        mapOfNodes.remove(endpoint);
        ring.remove(node.getUrl());
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(mapOfNodes.keySet());
    }

    private void installHandlers(HttpServer server, int localPort, Dao<byte[]> dao) {
        server.createContext("/v0/status", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, 0);
            } else {
                exchange.sendResponseHeaders(405, 0);
            }
            exchange.close();
        });

        server.createContext("/v0/entity", new ClusterProxyHandler(localPort, dao, ring));
    }
}
