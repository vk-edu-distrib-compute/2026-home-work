package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

final class LeaderMonitor {
    private static final Logger LOG = System.getLogger(LeaderMonitor.class.getName());

    private static final long PING_INTERVAL_MS = 300L;
    private static final long LEADER_TIMEOUT_MS = 1200L;

    private final Node node;

    private long lastPingTime;

    LeaderMonitor(Node node) {
        this.node = node;
    }

    void tick() {
        if (!node.isWorking()) {
            return;
        }

        int leaderId = node.getLeaderId();

        if (leaderId == Node.NO_LEADER || leaderId == node.getNodeId()) {
            return;
        }

        long now = System.currentTimeMillis();

        if (now - node.getLastLeaderAnswerTime() > LEADER_TIMEOUT_MS) {
            LOG.log(
                    Logger.Level.INFO,
                    node.getName() + " lost leader " + leaderId + ", starts election"
            );

            node.clearLeader();
            node.startElection();
            return;
        }

        if (now - lastPingTime >= PING_INTERVAL_MS) {
            node.sendMessage(leaderId, MessageType.PING);
            lastPingTime = now;
        }
    }
}
