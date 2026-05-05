package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.maryarta.consensus.Cluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

public class ConsensusTest {
    private static final Logger log = LoggerFactory.getLogger(ConsensusTest.class);
    private static final long LEADER_WAIT_TIMEOUT_MS = 10_000;
    private static final long POLL_INTERVAL_MS = 100;

    @Test
    public void shouldElectMaxAvailableNodeAsLeader() throws InterruptedException {
        List<Integer> ids = List.of(42, 75, 82, 13);
        int maxId = maxId(ids);
        int secondMaxId = secondMax(ids);
        int observerNodeId = min(ids);
        Cluster cluster = new Cluster(ids);
        waitUntilLeaderIs(cluster, observerNodeId, maxId);
        Assertions.assertEquals(maxId, cluster.nodes.get(observerNodeId).getLeaderID());
        cluster.stopNode(maxId);
        log.info("Node {} was stopped", maxId);
        waitUntilLeaderIs(cluster, observerNodeId, secondMaxId);
        Assertions.assertEquals(secondMaxId, cluster.nodes.get(observerNodeId).getLeaderID());
        cluster.startNode(maxId);
        log.info("Node {} was started", maxId);
        waitUntilLeaderIs(cluster, observerNodeId, maxId);
        Assertions.assertEquals(maxId,cluster.nodes.get(observerNodeId).getLeaderID());
    }

    private void waitUntilLeaderIs(Cluster cluster, int observerNodeId, int expectedLeaderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + LEADER_WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int actualLeaderId = cluster.nodes.get(observerNodeId).getLeaderID();
            if (actualLeaderId == expectedLeaderId) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
    }

    private int maxId(List<Integer> ids){
        return ids.stream()
                .max(Integer::compareTo)
                .orElseThrow();
    }

    private int secondMax(List<Integer> ids) {
        return ids.stream()
                .distinct()
                .sorted(Comparator.reverseOrder())
                .skip(1)
                .findFirst()
                .orElseThrow();
    }
    private int min(List<Integer> ids) {
        return ids.stream()
                .min(Integer::compareTo)
                .orElseThrow();

    }
}
