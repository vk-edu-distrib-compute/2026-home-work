package company.vk.edu.distrib.compute.artsobol.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ConsensusApplication {
    private static final Logger log = LoggerFactory.getLogger(ConsensusApplication.class);
    private static final int NO_ARGUMENTS = 0;
    private static final int MIN_NODE_COUNT = 1;
    private static final int DEFAULT_NODE_COUNT = 5;
    private static final long INITIAL_STATE_DELAY_MS = 1_500L;
    private static final long FAILURE_STATE_DELAY_MS = 1_500L;
    private static final long RECOVERY_STATE_DELAY_MS = 1_500L;
    private static final long SHUTDOWN_STATE_DELAY_MS = 1_500L;

    private ConsensusApplication() {
    }

    public static void main(String... args) throws InterruptedException {
        int nodeCount = args.length == NO_ARGUMENTS ? DEFAULT_NODE_COUNT : Integer.parseInt(args[0]);
        try (
                ConsensusCluster cluster = new ConsensusCluster(nodeIds(nodeCount));
                ConsensusVisualizer visualizer = new ConsensusVisualizer(cluster, Duration.ofMillis(500L))
        ) {
            cluster.start();
            visualizer.start();
            sleepAndLogLeader(INITIAL_STATE_DELAY_MS, cluster);

            int failedLeader = cluster.leaderId();
            cluster.failNode(failedLeader);
            sleepAndLogLeader(FAILURE_STATE_DELAY_MS, cluster);

            cluster.restoreNode(failedLeader);
            sleepAndLogLeader(RECOVERY_STATE_DELAY_MS, cluster);

            cluster.gracefulShutdownLeader();
            sleepAndLogLeader(SHUTDOWN_STATE_DELAY_MS, cluster);
        }
    }

    private static void sleepAndLogLeader(long millis, ConsensusCluster cluster) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (log.isDebugEnabled()) {
                log.debug("Leader is {}", cluster.leaderId());
            }
            TimeUnit.MILLISECONDS.sleep(500L);
        }
    }

    private static List<Integer> nodeIds(int nodeCount) {
        if (nodeCount < MIN_NODE_COUNT) {
            throw new IllegalArgumentException("Node count must be positive");
        }
        List<Integer> nodeIds = new ArrayList<>(nodeCount);
        for (int nodeId = 1; nodeId <= nodeCount; nodeId++) {
            nodeIds.add(nodeId);
        }
        return nodeIds;
    }
}
