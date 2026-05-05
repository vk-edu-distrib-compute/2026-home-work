package company.vk.edu.distrib.compute.artsobol.consensus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public final class ConsensusCluster implements AutoCloseable {
    public static final int NO_LEADER = -1;

    private final ReentrantLock leaderLock = new ReentrantLock();
    private final Map<Integer, ConsensusNode> nodes;
    private final List<Integer> sortedNodeIds;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicInteger leaderId = new AtomicInteger(NO_LEADER);

    public ConsensusCluster(Collection<Integer> nodeIds) {
        this(nodeIds, ConsensusConfig.defaults());
    }

    public ConsensusCluster(Collection<Integer> nodeIds, ConsensusConfig config) {
        Objects.requireNonNull(nodeIds);
        Objects.requireNonNull(config);
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("Cluster must contain at least one node");
        }
        Set<Integer> uniqueIds = new TreeSet<>(nodeIds);
        if (uniqueIds.size() != nodeIds.size()) {
            throw new IllegalArgumentException("Node IDs must be unique");
        }
        Map<Integer, ConsensusNode> createdNodes = new ConcurrentHashMap<>();
        for (int nodeId : uniqueIds) {
            createdNodes.put(nodeId, new ConsensusNode(nodeId, this, config));
        }
        this.nodes = Collections.unmodifiableMap(createdNodes);
        this.sortedNodeIds = List.copyOf(uniqueIds);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        for (ConsensusNode node : nodes.values()) {
            node.start();
        }
        for (ConsensusNode node : nodes.values()) {
            node.requestElection();
        }
    }

    public void failNode(int nodeId) {
        ConsensusNode node = node(nodeId);
        node.fail();
        leaderLock.lock();
        try {
            if (leaderId.get() == nodeId) {
                leaderId.set(NO_LEADER);
            }
        } finally {
            leaderLock.unlock();
        }
    }

    public void restoreNode(int nodeId) {
        ConsensusNode node = node(nodeId);
        node.restore();
        node.requestElection();
    }

    public void gracefulShutdownLeader() {
        int currentLeaderId = leaderId();
        if (currentLeaderId == NO_LEADER) {
            return;
        }

        ConsensusNode leader = node(currentLeaderId);
        if (!leader.active()) {
            return;
        }

        for (ConsensusNode node : nodes.values()) {
            if (node.nodeId() != currentLeaderId) {
                node.notifyLeaderShutdown(currentLeaderId);
            }
        }

        leader.shutdownGracefully();
        leaderLock.lock();
        try {
            if (leaderId.get() == currentLeaderId) {
                leaderId.set(NO_LEADER);
            }
        } finally {
            leaderLock.unlock();
        }
        requestElectionFromActiveNodes();
    }

    public int leaderId() {
        return leaderId.get();
    }

    public Map<Integer, NodeState> snapshot() {
        Map<Integer, NodeState> snapshot = new ConcurrentHashMap<>();
        for (ConsensusNode node : nodes.values()) {
            snapshot.put(node.nodeId(), node.state());
        }
        return Collections.unmodifiableMap(snapshot);
    }

    @Override
    public void close() throws InterruptedException {
        for (ConsensusNode node : nodes.values()) {
            node.stopNode();
        }
        for (ConsensusNode node : nodes.values()) {
            node.join();
        }
    }

    void send(int targetId, Message message) {
        ConsensusNode target = nodes.get(targetId);
        if (target != null) {
            target.receive(message);
        }
    }

    boolean publishVictory(int candidateId) {
        leaderLock.lock();
        try {
            if (highestActiveNodeIdLocked() != candidateId) {
                return false;
            }
            leaderId.set(candidateId);
        } finally {
            leaderLock.unlock();
        }
        Message victory = Message.victory(candidateId);
        for (ConsensusNode node : nodes.values()) {
            node.receive(victory);
        }
        return true;
    }

    boolean hasHigherActiveNode(int nodeId) {
        for (int higherId : higherNodeIds(nodeId)) {
            ConsensusNode node = nodes.get(higherId);
            if (node != null && node.active()) {
                return true;
            }
        }
        return false;
    }

    boolean isHighestActive(int nodeId) {
        leaderLock.lock();
        try {
            return highestActiveNodeIdLocked() == nodeId;
        } finally {
            leaderLock.unlock();
        }
    }

    List<Integer> higherNodeIds(int nodeId) {
        List<Integer> result = new ArrayList<>();
        for (int currentId : sortedNodeIds) {
            if (currentId > nodeId) {
                result.add(currentId);
            }
        }
        return result;
    }

    private ConsensusNode node(int nodeId) {
        ConsensusNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node ID: " + nodeId);
        }
        return node;
    }

    private void requestElectionFromActiveNodes() {
        for (ConsensusNode node : nodes.values()) {
            if (node.active()) {
                node.requestElection();
            }
        }
    }

    private int highestActiveNodeIdLocked() {
        int highest = NO_LEADER;
        for (int nodeId : sortedNodeIds) {
            ConsensusNode node = nodes.get(nodeId);
            if (node != null && node.active()) {
                highest = nodeId;
            }
        }
        return highest;
    }
}
