package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MarinchankaKVCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(MarinchankaKVCluster.class);
    private static final int VIRTUAL_NODES = 100;

    private final Map<String, MarinchankaKVService> nodes = new ConcurrentHashMap<>();
    private final Map<String, PersistentDao> daos = new ConcurrentHashMap<>();
    private final ConsistentHashingRouter router;
    private final HttpClient httpClient = new HttpClient();

    public MarinchankaKVCluster(List<Integer> ports, String baseDataDir) {
        this.router = new ConsistentHashingRouter(VIRTUAL_NODES);

        for (int port : ports) {
            ClusterNode node = new ClusterNode("localhost", port);
            router.addNode(node);

            try {
                String nodeDataDir = baseDataDir + "/node_" + port;
                PersistentDao dao = new PersistentDao(nodeDataDir);
                daos.put(node.getEndpoint(), dao);

                MarinchankaKVService service = new MarinchankaKVService(port, dao, router);
                nodes.put(node.getEndpoint(), service);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create DAO for port " + port, e);
            }
        }

        log.info("Created cluster with nodes: {}", nodes.keySet());
    }

    @Override
    public void start() {
        log.info("Starting all cluster nodes");
        nodes.values().forEach(MarinchankaKVService::start);
    }

    @Override
    public void start(String endpoint) {
        String cleanEndpoint = endpoint.replace("http://", "");
        log.info("Starting node: {}", cleanEndpoint);
        MarinchankaKVService service = nodes.get(cleanEndpoint);
        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        PersistentDao dao = daos.get(cleanEndpoint);
        if (dao != null) {
            try {
                dao.get("__test__");
            } catch (IOException e) {
                log.info("Recreating DAO for {}: {}", cleanEndpoint, e.getMessage());
                try {
                    int port = Integer.parseInt(cleanEndpoint.split(":")[1]);
                    dao = new PersistentDao("./cluster_data/node_" + port);
                    daos.put(cleanEndpoint, dao);
                    service = new MarinchankaKVService(port, dao, router);
                    nodes.put(cleanEndpoint, service);
                } catch (Exception ex) {
                    RuntimeException re = new RuntimeException("Failed to recreate DAO: " + ex.getMessage(), ex);
                    re.addSuppressed(e);
                    throw re;
                }
            } catch (NoSuchElementException e) {
                // Ключ не найден, DAO работает
            } catch (Exception e) {
                log.debug("Test query failed, but DAO may still be ok", e);
            }
        }

        service.start();
    }

    @Override
    public void stop() {
        log.info("Stopping all cluster nodes");
        nodes.values().forEach(MarinchankaKVService::stop);
        daos.values().forEach(dao -> {
            try {
                dao.close();
            } catch (IOException e) {
                log.error("Error closing DAO", e);
            }
        });
    }

    @Override
    public void stop(String endpoint) {
        String cleanEndpoint = endpoint.replace("http://", "");
        log.info("Stopping node: {}", cleanEndpoint);
        MarinchankaKVService service = nodes.get(cleanEndpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return nodes.keySet().stream()
                .map(endpoint -> "http://" + endpoint)
                .toList();
    }

    // Методы для работы с данными

    public byte[] get(String key) {
        ClusterNode responsibleNode = router.getNode(key);
        PersistentDao dao = daos.get(responsibleNode.getEndpoint());

        if (dao != null) {
            try {
                return dao.get(key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get key: " + key, e);
            }
        } else {
            return httpClient.get(responsibleNode, key);
        }
    }

    public void put(String key, byte[] value) {
        ClusterNode responsibleNode = router.getNode(key);
        PersistentDao dao = daos.get(responsibleNode.getEndpoint());

        if (dao != null) {
            try {
                dao.upsert(key, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to put key: " + key, e);
            }
        } else {
            httpClient.put(responsibleNode, key, value);
        }
    }

    public void delete(String key) {
        ClusterNode responsibleNode = router.getNode(key);
        PersistentDao dao = daos.get(responsibleNode.getEndpoint());

        if (dao != null) {
            try {
                dao.delete(key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete key: " + key, e);
            }
        } else {
            httpClient.delete(responsibleNode, key);
        }
    }

    public ConsistentHashingRouter getRouter() {
        return router;
    }

    public boolean isLocalPort(int port) {
        return nodes.containsKey("localhost:" + port);
    }
}
