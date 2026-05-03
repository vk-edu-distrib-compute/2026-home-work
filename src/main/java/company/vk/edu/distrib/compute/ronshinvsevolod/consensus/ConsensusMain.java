package company.vk.edu.distrib.compute.ronshinvsevolod.consensus;

import java.util.logging.Logger;

// Demonstrates cluster leader election.
public final class ConsensusMain {

    private static final Logger LOG = Logger.getLogger(ConsensusMain.class.getName());
    private static final int CLUSTER_SIZE = 5;
    private static final int SETTLE_MS = 1500;

    private ConsensusMain() {
    }

    public static void main(final String[] args) throws InterruptedException {
        final Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();

        Thread.sleep(SETTLE_MS);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("leader after start: " + cluster.getLeaderId());
        }

        cluster.failNode(CLUSTER_SIZE);
        Thread.sleep(SETTLE_MS);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("leader after node " + CLUSTER_SIZE + " failed: " + cluster.getLeaderId());
        }

        cluster.recoverNode(CLUSTER_SIZE);
        Thread.sleep(SETTLE_MS);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("leader after node " + CLUSTER_SIZE + " recovered: " + cluster.getLeaderId());
        }

        cluster.failNode(CLUSTER_SIZE);
        cluster.failNode(CLUSTER_SIZE - 1);
        Thread.sleep(SETTLE_MS);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("leader after nodes " + (CLUSTER_SIZE - 1) + " and " + CLUSTER_SIZE + " failed: "
                + cluster.getLeaderId());
        }

        cluster.stop();
    }
}
