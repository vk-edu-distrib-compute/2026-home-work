package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Кластер из нескольких нод {@link KVServiceProxyImpl}.
 * Каждая нода знает про все остальные (список allEndpoints передаётся при создании).
 */
public class ShuuuurikKVCluster implements KVCluster {

    private static final Logger log = LoggerFactory.getLogger(ShuuuurikKVCluster.class);

    /**
     * endpoint -> KVService этой ноды.
     */
    private final Map<String, KVService> nodes;

    /**
     * Упорядоченный список endpoint'ов.
     */
    private final List<String> endpoints;

    public ShuuuurikKVCluster(Map<String, KVService> nodes, List<String> endpoints) {
        this.nodes = new ConcurrentHashMap<>(nodes);
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
        log.info("Cluster started: {}", endpoints);
    }

    @Override
    public void start(String endpoint) {
        KVService node = getNode(endpoint);
        node.start();
        log.info("Node started: {}", endpoint);
    }

    @Override
    public void stop() {
        for (int i = endpoints.size() - 1; i >= 0; i--) {
            stopQuietly(endpoints.get(i));
        }
        log.info("Cluster stopped");
    }

    @Override
    public void stop(String endpoint) {
        KVService node = getNode(endpoint);
        node.stop();
        log.info("Node stopped: {}", endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private KVService getNode(String endpoint) {
        KVService node = nodes.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return node;
    }

    /**
     * Останавливает ноду, подавляя исключения - используется при остановке всего кластера,
     * чтобы одна сломанная нода не помешала остановить остальные.
     */
    private void stopQuietly(String endpoint) {
        try {
            stop(endpoint);
        } catch (Exception e) {
            log.warn("Error stopping node {}", endpoint, e);
        }
    }
}
