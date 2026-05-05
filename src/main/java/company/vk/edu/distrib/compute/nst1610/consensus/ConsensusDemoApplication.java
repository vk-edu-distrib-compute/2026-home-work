package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.List;

public final class ConsensusDemoApplication {

    public static void main(String[] args) throws Exception {
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

    private static void runScenario(String name, ConsensusConfig config, ScenarioAction action) throws Exception {
        System.out.println();
        System.out.println("Scenario: " + name);
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, config)) {
            cluster.start();
            action.run(cluster);
        }
    }

    private static void printSnapshots(List<NodeSnapshot> snapshots) {
        for (NodeSnapshot snapshot : snapshots) {
            System.out.println(snapshot.toString());
        }
    }

    @FunctionalInterface
    private interface ScenarioAction {
        void run(ConsensusCluster cluster) throws Exception;
    }
}
