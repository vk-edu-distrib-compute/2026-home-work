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

    public enum Algorithm {
        CONSISTENT_HASHING,
        RENDEZVOUS_HASHING
    }

    private final Map<String, MarinchankaKVService> nodes = new ConcurrentHashMap<>();
    private final Map<String, PersistentDao> daos = new ConcurrentHashMap<>();
    private final ShardingRouter router;
    private final HttpClient httpClient = new HttpClient();
    private final Algorithm algorithm;

    public MarinchankaKVCluster(List<Integer> ports, String baseDataDir) {
        this(ports, baseDataDir, Algorithm.CONSISTENT_HASHING);
    }

    public MarinchankaKVCluster(List<Integer> ports, String baseDataDir, Algorithm algorithm) {
        this.algorithm = algorithm;
        this.router = createRouter(algorithm);
        String dataDir = baseDataDir;

        for (int port : ports) {
            addNode(port, dataDir);
        }

        if (log.isInfoEnabled()) {
            log.info("Created cluster with algorithm {} and nodes: {}", algorithm, nodes.keySet());
        }
    }

    private ShardingRouter createRouter(Algorithm algorithm) {
        return switch (algorithm) {
            case CONSISTENT_HASHING -> new ConsistentHashingRouter(VIRTUAL_NODES);
            case RENDEZVOUS_HASHING -> new RendezvousHashingRouter();
        };
    }

    private void addNode(int port, String dataDir) {
        ClusterNode node = new ClusterNode("localhost", port);
        router.addNode(node);

        try {
            String nodeDataDir = dataDir + "/node_" + port;
            PersistentDao dao = new PersistentDao(nodeDataDir);
            daos.put(node.getEndpoint(), dao);

            MarinchankaKVService service = new MarinchankaKVService(port, dao, router);
            nodes.put(node.getEndpoint(), service);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create DAO for port " + port, e);
        }
    }

    @Override
    public void start() {
        if (log.isInfoEnabled()) {
            log.info("Starting all cluster nodes");
        }
        nodes.values().forEach(MarinchankaKVService::start);
    }

    @Override
    public void start(String endpoint) {
        String cleanEndpoint = endpoint.replace("http://", "");
        if (log.isInfoEnabled()) {
            log.info("Starting node: {}", cleanEndpoint);
        }

        MarinchankaKVService service = nodes.get(cleanEndpoint);
        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        ensureDaoIsWorking(cleanEndpoint);
        nodes.get(cleanEndpoint).start();
    }

    private void ensureDaoIsWorking(String cleanEndpoint) {
        PersistentDao dao = daos.get(cleanEndpoint);
        if (dao == null) {
            return;
        }

        try {
            dao.get("__test__");
        } catch (IOException e) {
            if (log.isInfoEnabled()) {
                log.info("Recreating DAO for {}: {}", cleanEndpoint, e.getMessage());
            }
            recreateDao(cleanEndpoint, e);
        } catch (NoSuchElementException ignored) {
            // Key not found - DAO is working
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Test query failed, but DAO may still be ok", e);
            }
        }
    }

    private void recreateDao(String cleanEndpoint, IOException cause) {
        try {
            int port = Integer.parseInt(cleanEndpoint.split(":")[1]);
            PersistentDao dao = new PersistentDao("./cluster_data/node_" + port);
            daos.put(cleanEndpoint, dao);
            MarinchankaKVService service = new MarinchankaKVService(port, dao, router);
            nodes.put(cleanEndpoint, service);
        } catch (Exception ex) {
            IllegalStateException re = new IllegalStateException(
                    "Failed to recreate DAO: " + ex.getMessage(), ex);
            re.addSuppressed(cause);
            throw re;
        }
    }

    @Override
    public void stop() {
        if (log.isInfoEnabled()) {
            log.info("Stopping all cluster nodes");
        }
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
        if (log.isInfoEnabled()) {
            log.info("Stopping node: {}", cleanEndpoint);
        }
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

    public byte[] get(String key) {
        ClusterNode responsibleNode = router.getNode(key);
        PersistentDao dao = daos.get(responsibleNode.getEndpoint());

        if (dao != null) {
            try {
                return dao.get(key);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to get key: " + key, e);
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
                throw new IllegalStateException("Failed to put key: " + key, e);
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
                throw new IllegalStateException("Failed to delete key: " + key, e);
            }
        } else {
            httpClient.delete(responsibleNode, key);
        }
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public boolean isLocalPort(int port) {
        return nodes.containsKey("localhost:" + port);
    }
}
