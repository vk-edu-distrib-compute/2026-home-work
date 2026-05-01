package company.vk.edu.distrib.compute.vitos23.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
public final class SimulationRunner {

    private static final Logger log = LoggerFactory.getLogger(SimulationRunner.class);
    private static final int ONE = 1; // Just for Codacy

    private SimulationRunner() {
    }

    public static void main(String... args) throws InterruptedException {
        boolean gui = Arrays.asList(args).contains("--gui");

        log.info("Starting simulation with {} nodes", SimulationProperties.NODE_COUNT);

        Map<Integer, Node> nodeById = new ConcurrentHashMap<>();
        MessageSender messageSender = new MessageSender(nodeById);

        for (int i = 0; i < SimulationProperties.NODE_COUNT; i++) {
            Node node = new Node(i, messageSender);
            nodeById.put(i, node);
        }

        final List<Thread> threads = startNodes(nodeById);

        if (gui) {
            new ClusterVisualizer().show(nodeById);
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(
                () -> validateAndLogClusterState(nodeById),
                SimulationProperties.STATE_LOG_INTERVAL.toMillis(),
                SimulationProperties.STATE_LOG_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS
        );
        scheduler.scheduleAtFixedRate(
                () -> randomFailure(nodeById),
                SimulationProperties.FAILURE_INTERVAL.toMillis(),
                SimulationProperties.FAILURE_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down simulation...");
            scheduler.shutdownNow();
            threads.forEach(Thread::interrupt);
        }));

        Thread.currentThread().join();
    }

    private static List<Thread> startNodes(Map<Integer, Node> nodeById) {
        List<Thread> threads = new ArrayList<>();
        for (Node node : nodeById.values()) {
            Thread thread = Thread.ofPlatform()
                    .name("node-" + node.getId())
                    .daemon()
                    .start(() -> {
                        try {
                            node.start();
                        } catch (InterruptedException e) {
                            // no action needed
                        }
                    });
            threads.add(thread);
        }
        return threads;
    }

    private static void validateAndLogClusterState(Map<Integer, Node> nodeById) {
        long leaderCount = nodeById.values().stream()
                .filter(node -> node.getStatus() == NodeStatus.LEADER)
                .count();
        if (leaderCount > ONE) {
            log.error("Illegal cluster state: encountered {} leaders", leaderCount);
        }

        if (log.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder("Cluster state: [");
            for (Node node : nodeById.values()) {
                sb.append(String.format(
                        "%d:%s ",
                        node.getId(),
                        node.getStatus()
                ));
            }
            sb.append("] leaderId=");
            for (Node node : nodeById.values()) {
                if (!node.isBroken() && node.getLeaderId() != null) {
                    sb.append(node.getLeaderId());
                    break;
                }
            }
            log.info(sb.toString());
        }
    }

    private static void randomFailure(Map<Integer, Node> nodeById) {
        for (Node node : nodeById.values()) {
            if (Math.random() < SimulationProperties.FAILURE_PROBABILITY) {
                boolean currentlyBroken = node.isBroken();
                int nodeId = node.getId();
                if (currentlyBroken) {
                    log.info("Recovering node {}", nodeId);
                    node.setBroken(false);
                } else {
                    log.info("Breaking node {}", nodeId);
                    node.setBroken(true);
                }
            }
        }
    }
}
