package company.vk.edu.distrib.compute.dariaprindina.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public final class ConsensusSimulationApp {
    private static final Logger log = LoggerFactory.getLogger(ConsensusSimulationApp.class);

    private ConsensusSimulationApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        final ConsensusClusterConfig config = new ConsensusClusterConfig(
            Duration.ofMillis(500),
            Duration.ofMillis(400),
            Duration.ofSeconds(2),
            0.03
        );
        final ConsensusCluster cluster = new ConsensusCluster(List.of(1, 2, 3, 4, 5), config);
        cluster.start();

        log.info("Cluster started, waiting initial leader election");
        Thread.sleep(3000);
        printState(cluster);

        final Integer leader = cluster.currentLeader();
        if (leader != null) {
            log.info("Force down current leader={}", leader);
            cluster.forceDown(leader);
            Thread.sleep(3000);
            printState(cluster);
            log.info("Restore previous leader={}", leader);
            cluster.forceUp(leader);
        }

        Thread.sleep(5000);
        printState(cluster);
        cluster.shutdown();
        log.info("Cluster stopped");
    }

    private static void printState(ConsensusCluster cluster) {
        if (log.isInfoEnabled()) {
            final Integer leader = cluster.currentLeader();
            log.info("Current leader={}", leader);
            for (ConsensusNodeSnapshot snapshot : cluster.snapshots()) {
                log.info(
                    "Node {}: state={}, knownLeader={}",
                    snapshot.nodeId(),
                    snapshot.state(),
                    snapshot.knownLeaderId()
                );
            }
        }
    }
}
