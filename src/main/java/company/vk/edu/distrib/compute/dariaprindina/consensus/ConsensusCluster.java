package company.vk.edu.distrib.compute.dariaprindina.consensus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConsensusCluster {
    private final Map<Integer, ConsensusNode> nodesById;
    private final List<Integer> orderedIds;

    public ConsensusCluster(List<Integer> nodeIds, ConsensusClusterConfig config) {
        Objects.requireNonNull(nodeIds, "nodeIds");
        Objects.requireNonNull(config, "config");
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("nodeIds must not be empty");
        }
        this.orderedIds = nodeIds.stream().sorted().distinct().toList();
        if (orderedIds.size() != nodeIds.size()) {
            throw new IllegalArgumentException("node IDs must be unique");
        }
        this.nodesById = new HashMap<>();
        for (Integer id : orderedIds) {
            nodesById.put(id, new ConsensusNode(id, this, orderedIds, config));
        }
    }

    public synchronized void start() {
        for (ConsensusNode node : nodesById.values()) {
            node.startNodeThread();
        }
    }

    public synchronized void shutdown() {
        for (ConsensusNode node : nodesById.values()) {
            node.shutdown();
        }
    }

    public boolean send(int targetNodeId, ConsensusMessage message) {
        final ConsensusNode node = nodesById.get(targetNodeId);
        return node != null && node.enqueue(message);
    }

    public void broadcast(ConsensusMessage message) {
        for (Integer id : orderedIds) {
            if (id == message.senderId()) {
                continue;
            }
            send(id, message);
        }
    }

    public synchronized void forceDown(int nodeId) {
        getNodeOrThrow(nodeId).forceDown();
    }

    public synchronized void forceUp(int nodeId) {
        getNodeOrThrow(nodeId).forceUp();
    }

    public synchronized List<ConsensusNodeSnapshot> snapshots() {
        final List<ConsensusNodeSnapshot> snapshots = new ArrayList<>();
        for (Integer id : orderedIds) {
            snapshots.add(getNodeOrThrow(id).snapshot());
        }
        return snapshots;
    }

    public synchronized Integer currentLeader() {
        Integer maxLeader = null;
        for (ConsensusNodeSnapshot snapshot : snapshots()) {
            if (snapshot.state() == ConsensusNodeState.DOWN || snapshot.knownLeaderId() == null) {
                continue;
            }
            final int candidate = snapshot.knownLeaderId();
            if (maxLeader == null || candidate > maxLeader) {
                maxLeader = candidate;
            }
        }
        return maxLeader;
    }

    private ConsensusNode getNodeOrThrow(int nodeId) {
        final ConsensusNode node = nodesById.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node id: " + nodeId);
        }
        return node;
    }
}
