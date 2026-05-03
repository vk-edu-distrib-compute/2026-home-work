package company.vk.edu.distrib.compute.dariaprindina.consensus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.AvoidSynchronizedStatement")
public class ConsensusCluster {
    private final Map<Integer, ConsensusNode> nodesById;
    private final List<Integer> orderedIds;
    private final Object clusterLock;

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
        this.clusterLock = new Object();
        this.nodesById = orderedIds.stream().collect(Collectors.toMap(
            Function.identity(),
            id -> new ConsensusNode(id, this, orderedIds, config),
            (left, right) -> left,
            HashMap::new
        ));
    }

    public void start() {
        synchronized (clusterLock) {
            for (ConsensusNode node : nodesById.values()) {
                node.startNodeThread();
            }
        }
    }

    public void shutdown() {
        synchronized (clusterLock) {
            for (ConsensusNode node : nodesById.values()) {
                node.shutdown();
            }
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

    public void forceDown(int nodeId) {
        synchronized (clusterLock) {
            getNodeOrThrow(nodeId).forceDown();
        }
    }

    public void forceUp(int nodeId) {
        synchronized (clusterLock) {
            getNodeOrThrow(nodeId).forceUp();
        }
    }

    public List<ConsensusNodeSnapshot> snapshots() {
        synchronized (clusterLock) {
            final List<ConsensusNodeSnapshot> snapshots = new ArrayList<>();
            for (Integer id : orderedIds) {
                snapshots.add(getNodeOrThrow(id).snapshot());
            }
            return snapshots;
        }
    }

    public Integer currentLeader() {
        synchronized (clusterLock) {
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
    }

    private ConsensusNode getNodeOrThrow(int nodeId) {
        final ConsensusNode node = nodesById.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node id: " + nodeId);
        }
        return node;
    }
}
