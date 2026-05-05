package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

final class LeaderMonitor {
    private static final long PING_INTERVAL_MS = 1_000;
    private static final long ANSWER_TIMEOUT_MS = 700;

    private final Node node;
    private long lastPingTime;
    private long lastAnswerFromLeaderTime;

    LeaderMonitor(Node node) {
        this.node = node;
    }

    void resetTimers() {
        lastPingTime = 0;
        lastAnswerFromLeaderTime = System.currentTimeMillis();
    }

    void markLeaderAnswered() {
        lastAnswerFromLeaderTime = System.currentTimeMillis();
    }

    void checkLeader() {
        if (!node.isAlive()) {
            return;
        }

        if (node.getLeaderId() == -1 && !node.getElectionManager().isInProgress()) {
            node.getElectionManager().startElection();
            return;
        }

        if (node.getState() == State.LEADER) {
            return;
        }

        Node leader = node.getCluster().get(node.getLeaderId());

        if (leader == null) {
            node.getElectionManager().startElection();
            return;
        }

        pingLeaderIfNeeded(leader);
        checkLeaderTimeout();
    }

    private void pingLeaderIfNeeded(Node leader) {
        long now = System.currentTimeMillis();

        if (now - lastPingTime < PING_INTERVAL_MS) {
            return;
        }

        lastPingTime = now;

        boolean delivered = node.sendMessageTo(
                leader,
                new Message(Message.Type.PING, node.getId())
        );

        if (!delivered) {
            ClusterLogger.event(
                    node.getId(),
                    "detected unavailable leader " + node.getLeaderId()
            );
            node.getElectionManager().startElection();
        }
    }

    private void checkLeaderTimeout() {
        long now = System.currentTimeMillis();

        if (lastPingTime <= 0 || lastAnswerFromLeaderTime == 0) {
            return;
        }

        if (now - lastAnswerFromLeaderTime > PING_INTERVAL_MS + ANSWER_TIMEOUT_MS * 2) {
            ClusterLogger.event(
                    node.getId(),
                    "timed out leader " + node.getLeaderId()
            );
            node.getElectionManager().startElection();
        }
    }
}
