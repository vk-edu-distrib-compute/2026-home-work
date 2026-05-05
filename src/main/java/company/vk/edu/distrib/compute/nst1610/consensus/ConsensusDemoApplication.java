package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsensusDemoApplication {
    private static final Logger log = LoggerFactory.getLogger(ConsensusDemoApplication.class);

    private ConsensusDemoApplication() {
    }

    public static void main(String[] args) throws InterruptedException {
        runScenario("startup", ConsensusConfig.defaultConfig(), cluster -> {
            Thread.sleep(1_500L);
            printSnapshots(cluster.snapshots());
        });
        runScenario("leader-failure", ConsensusConfig.defaultConfig(), cluster -> {
            Thread.sleep(1_200L);
            cluster.failNode(5);
            Thread.sleep(2_000L);
            printSnapshots(cluster.snapshots());
        });
        runScenario("recovery", ConsensusConfig.defaultConfig(), cluster -> {
            Thread.sleep(1_000L);
            cluster.failNode(5);
            Thread.sleep(1_400L);
            cluster.recoverNode(5);
            Thread.sleep(1_500L);
            printSnapshots(cluster.snapshots());
        });
        runScenario("frequent-failures", ConsensusConfig.unstableConfig(), cluster -> {
            Thread.sleep(4_500L);
            printSnapshots(cluster.snapshots());
        });
    }

    private static void runScenario(String name, ConsensusConfig config, ScenarioAction action)
        throws InterruptedException {
        log.info("");
        log.info("Scenario: {}", name);
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, config)) {
            cluster.start();
            action.run(cluster);
        }
    }

    private static void printSnapshots(List<NodeSnapshot> snapshots) {
        for (NodeSnapshot snapshot : snapshots) {
            log.info("{}", snapshot);
        }
    }

    @FunctionalInterface
    private interface ScenarioAction {
        void run(ConsensusCluster cluster) throws InterruptedException;
    }
}
