package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class GracefulShutdownManager {
    private final Node node;

    private final Lock shutdownLock = new ReentrantLock();

    GracefulShutdownManager(Node node) {
        this.node = node;
    }

    void gracefulShutdown() {
        shutdownLock.lock();
        try {
            gracefulShutdownInternal();
        } finally {
            shutdownLock.unlock();
        }
    }

    private void gracefulShutdownInternal() {
        if (!node.isAlive()) {
            return;
        }

        ClusterLogger.event(node.getId(), "requested graceful shutdown");

        if (node.getState() != State.LEADER) {
            ClusterLogger.event(node.getId(), "is not leader, shutting down normally");
            node.setEnabled(false);
            return;
        }

        boolean transferred = transferLeadership();

        if (!transferred) {
            ClusterLogger.event(
                    node.getId(),
                    "no alive replacement leader found, shutting down"
            );
            node.setEnabled(false);
            return;
        }

        node.setEnabled(false);
    }

    private boolean transferLeadership() {
        Node nextLeader = findHighestAliveNodeExceptSelf();

        while (nextLeader != null) {
            ClusterLogger.event(
                    node.getId(),
                    "transferring leadership to node " + nextLeader.getId()
            );

            boolean transferred = nextLeader.forceBecomeLeaderAfterTransfer();

            if (transferred) {
                broadcastVictory(nextLeader);
                return true;
            }

            ClusterLogger.event(
                    node.getId(),
                    "candidate node " + nextLeader.getId() + " failed during graceful transfer"
            );

            nextLeader = findHighestAliveNodeExcept(node.getId(), nextLeader.getId());
        }

        return false;
    }

    private void broadcastVictory(Node nextLeader) {
        Message victory = new Message(Message.Type.VICTORY, nextLeader.getId());

        for (Node clusterNode : node.getCluster().values()) {
            sendVictoryIfNeeded(clusterNode, nextLeader, victory);
        }
    }

    private Node findHighestAliveNodeExcept(int currentNodeId, int failedCandidateId) {
        Node result = null;

        for (Node clusterNode : node.getCluster().values()) {
            if (clusterNode.getId() == currentNodeId) {
                continue;
            }

            if (clusterNode.getId() == failedCandidateId) {
                continue;
            }

            if (!clusterNode.isAlive()) {
                continue;
            }

            if (result == null || clusterNode.getId() > result.getId()) {
                result = clusterNode;
            }
        }

        return result;
    }

    private void sendVictoryIfNeeded(Node clusterNode, Node nextLeader, Message victory) {
        if (clusterNode.getId() == node.getId()) {
            return;
        }

        if (clusterNode.getId() == nextLeader.getId()) {
            return;
        }

        node.sendMessageTo(clusterNode, victory);
    }

    private Node findHighestAliveNodeExceptSelf() {
        Node result = null;

        for (Node clusterNode : node.getCluster().values()) {
            result = selectBetterCandidate(result, clusterNode);
        }

        return result;
    }

    private Node selectBetterCandidate(Node current, Node candidate) {
        if (candidate.getId() == node.getId()) {
            return current;
        }

        if (!candidate.isAlive()) {
            return current;
        }

        if (current == null || candidate.getId() > current.getId()) {
            return candidate;
        }

        return current;
    }
}
