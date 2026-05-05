package company.vk.edu.distrib.compute.andeco.election;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ClusterMonitor extends Thread {
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String RED = "\u001B[31m";

    private final Cluster cluster;
    private final long periodMs;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ClusterMonitor(Cluster cluster, long periodMs) {
        super("election-monitor");
        this.cluster = cluster;
        this.periodMs = periodMs;
        setDaemon(true);
    }

    public void shutdown() {
        running.set(false);
        interrupt();
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                printSnapshot(cluster.nodes());
                TimeUnit.MILLISECONDS.sleep(periodMs);
            } catch (InterruptedException _) {
            }
        }
    }

    private static void printSnapshot(List<ElectionNode> nodes) {
        Integer leader = null;
        int alive = 0;
        for (ElectionNode node : nodes) {
            if (node.role() != NodeRole.DOWN) {
                alive++;
            }
            if (node.role() == NodeRole.LEADER) {
                if (leader == null) {
                    leader = node.id();
                } else if (leader != node.id()) {
                    leader = -1;
                }
            }
        }

        int maxAliveId = nodes.stream()
                .filter(n -> n.role() != NodeRole.DOWN)
                .map(ElectionNode::id)
                .max(Comparator.naturalOrder())
                .orElse(-1);

        String leaderText = leader == null ? "нет" : String.valueOf(leader);
        System.out.println("кластер: активных: " + alive + "/" + nodes.size()
                + " лидер: " + leaderText + " ожидаемо максимально активных:" + maxAliveId);

        for (ElectionNode node : nodes) {
            String color = switch (node.role()) {
                case LEADER -> GREEN;
                case SLAVE -> YELLOW;
                case DOWN -> RED;
            };
            System.out.println(color + "узел: " + node.id() + " роль: " + LocalisationUtils.roleRu(node.role()) + " лидер: " + node.leader()
                    + RESET);
        }
    }
}

