package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.korjick.app.node.CakeKVNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CakeKVCluster implements KVCluster, Closeable {
    private final Map<String, CakeKVNode> nodes;

    public CakeKVCluster(Map<String, CakeKVNode> nodes) {
        this.nodes = Map.copyOf(nodes);
    }

    @Override
    public void start() {
        nodes.values().forEach(CakeKVNode::start);
    }

    @Override
    public void start(String endpoint) {
        var node = nodes.get(endpoint);
        if (node != null) {
            node.start();
        }
    }

    @Override
    public void stop() {
        nodes.values().forEach(CakeKVNode::stop);
    }

    @Override
    public void stop(String endpoint) {
        CakeKVNode node = nodes.get(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public void close() throws IOException {
        for (CakeKVNode node : nodes.values()) {
            node.close();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return this.nodes.keySet().stream().toList();
    }
}
