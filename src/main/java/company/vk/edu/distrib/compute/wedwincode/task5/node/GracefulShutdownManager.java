package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

final class GracefulShutdownManager {
    private final Node node;

    GracefulShutdownManager(Node node) {
        this.node = node;
    }

    void gracefulShutdown() {
        synchronized (this) {
            gracefulShutdownInternal();
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

        Node nextLeader = findHighestAliveNodeExceptSelf();

        if (nextLeader == null) {
            ClusterLogger.event(node.getId(), "no alive replacement leader found, shutting down");
            node.setEnabled(false);
            return;
        }

        transferLeadership(nextLeader);
        node.setEnabled(false);
    }

    private void transferLeadership(Node nextLeader) {
        ClusterLogger.event(
                node.getId(),
                "transferring leadership to node " + nextLeader.getId()
        );

        nextLeader.forceBecomeLeaderAfterTransfer();

        Message victory = new Message(Message.Type.VICTORY, nextLeader.getId());

        for (Node clusterNode : node.getCluster().values()) {
            sendVictoryIfNeeded(clusterNode, nextLeader, victory);
        }
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
