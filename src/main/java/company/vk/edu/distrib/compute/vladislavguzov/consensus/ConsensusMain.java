package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsensusMain {

    private static final Logger log = LoggerFactory.getLogger(ConsensusMain.class);
    private static final int CLUSTER_SIZE = 5;
    private static final long STEP_WAIT_MS = 4000;

    public void execute() throws InterruptedException {
        Cluster cluster = new Cluster(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            cluster.getNode(i).setRandomFailuresEnabled(true);
        }

        log.info("=== Starting cluster of {} nodes ===", CLUSTER_SIZE);
        cluster.start();
        Thread.sleep(STEP_WAIT_MS);

        log.info("=== Initial state ===");
        cluster.printStatus();

        int leaderId = cluster.getCurrentLeaderId();
        log.info("=== Forcing leader (node {}) DOWN ===", leaderId);
        cluster.getNode(leaderId).forceDown();
        Thread.sleep(STEP_WAIT_MS);

        log.info("=== After leader failure ===");
        cluster.printStatus();

        log.info("=== Recovering node {} ===", leaderId);
        cluster.getNode(leaderId).forceUp();
        Thread.sleep(STEP_WAIT_MS);

        log.info("=== After recovery ===");
        cluster.printStatus();

        cluster.stop();
    }

    public static void main(String[] args) throws InterruptedException {
        new ConsensusMain().execute();
    }
}
