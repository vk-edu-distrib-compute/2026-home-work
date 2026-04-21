package company.vk.edu.distrib.compute.proteusp;

import company.vk.edu.distrib.compute.KVCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProteuspKVCluster implements KVCluster {

    Map<String, ProteuspKVNode> nodes;
    List<String> runningEndpoints;

    public ProteuspKVCluster(Map<String, ProteuspKVNode> nodes) {
        this.runningEndpoints = new CopyOnWriteArrayList<>();
        this.nodes = nodes;
    }

    @Override
    public void start() {
        for (Map.Entry<String, ProteuspKVNode> entry : nodes.entrySet()) {
            start(entry.getKey());
        }
    }

    @Override
    public void start(String endpoint) {
        ProteuspKVNode node = nodes.get(endpoint);
        if (!runningEndpoints.contains(endpoint)) {
            runningEndpoints.add(endpoint);
            node.activateNode();
        }
    }

    @Override
    public void stop() {
        for (Map.Entry<String, ProteuspKVNode> entry : nodes.entrySet()) {
            stop(entry.getKey());
        }
    }

    @Override
    public void stop(String endpoint) {
        ProteuspKVNode node = nodes.get(endpoint);
        if (runningEndpoints.contains(endpoint)) {
            runningEndpoints.remove(endpoint);
            node.deactivateNode();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(nodes.keySet());
    }
}
