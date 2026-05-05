package company.vk.edu.distrib.compute.arseniy90.consensus;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionManager.class.getName());

    private final int id;
    private final Map<Integer, Node> cluster;
    private final NodeState state;
    private final ScheduledExecutorService scheduler;

    public ElectionManager(int id, Map<Integer, Node> cluster, NodeState state, ScheduledExecutorService scheduler) {
        this.id = id;
        this.cluster = cluster;
        this.state = state;
        this.scheduler = scheduler;
    }

    public void startElection() {
        if (!state.compareAndSetElection(false, true)) {
            return;
        }

        int delay = ThreadLocalRandom.current().nextInt(300, 1000);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        state.setRole(NodeRole.CANDIDATE);
        LOGGER.info("Node {}: start elections...", id);
        state.resetLeaderContactTime();

        cluster.forEach((k, node) -> {
            if (k > id) {
                node.receiveMessage(new Message(MessageType.ELECT, id, "elect msg"));
            }
        });

        scheduler.schedule(() -> {
            if (state.compareAndSetElection(true, false)) {
                state.setLeaderId(id);
                state.setRole(NodeRole.LEADER);
                LOGGER.info("Node {}: is leader", id);

                cluster.forEach((k, node) -> {
                    if (k != id) {
                        node.receiveMessage(new Message(MessageType.VICTORY, id, "victory msg"));
                    }
                });
            }
        }, 1500, TimeUnit.MILLISECONDS);
    }
}
