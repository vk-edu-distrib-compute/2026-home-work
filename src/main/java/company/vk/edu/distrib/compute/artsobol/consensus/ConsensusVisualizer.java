package company.vk.edu.distrib.compute.artsobol.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConsensusVisualizer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConsensusVisualizer.class);
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String BLUE = "\u001B[34m";
    private static final String RED = "\u001B[31m";

    private final ConsensusCluster cluster;
    private final Duration interval;
    private final AtomicBoolean started = new AtomicBoolean();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "consensus-visualizer");
        thread.setDaemon(true);
        return thread;
    });

    public ConsensusVisualizer(ConsensusCluster cluster, Duration interval) {
        this.cluster = Objects.requireNonNull(cluster);
        this.interval = Objects.requireNonNull(interval);
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("Visualization interval must be positive");
        }
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executor.scheduleAtFixedRate(this::render, 0L, interval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public static String renderSnapshot(Map<Integer, NodeState> snapshot) {
        List<NodeState> sortedSnapshot = new ArrayList<>(snapshot.values());
        sortedSnapshot.sort(Comparator.comparingInt(NodeState::nodeId));
        StringBuilder builder = new StringBuilder(128).append("cluster");
        for (NodeState state : sortedSnapshot) {
            builder.append(" [id=")
                    .append(state.nodeId())
                    .append(",role=")
                    .append(coloredRole(state.role()))
                    .append(",leader=")
                    .append(state.leaderId())
                    .append(']');
        }
        return builder.toString();
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void render() {
        log.info("{}", renderSnapshot(cluster.snapshot()));
    }

    private static String coloredRole(NodeRole role) {
        return switch (role) {
            case LEADER -> GREEN + role + RESET;
            case FOLLOWER -> BLUE + role + RESET;
            case DOWN -> RED + role + RESET;
        };
    }
}
