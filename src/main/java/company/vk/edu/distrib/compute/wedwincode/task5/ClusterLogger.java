package company.vk.edu.distrib.compute.wedwincode.task5;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public final class ClusterLogger {
    private static final String RESET = "\u001B[0m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String GRAY = "\u001B[90m";

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    private ClusterLogger() {
    }

    public static synchronized void event(int nodeId, String event) {
        String time = LocalTime.now().format(TIME_FORMAT);

        System.out.println(
                GRAY + "[" + time + "] " + RESET
                        + BLUE + "{node=" + nodeId + "} " + RESET
                        + event
        );
    }

    public static synchronized void clusterSnapshot(Map<Integer, Node> cluster) {
        StringBuilder builder = new StringBuilder();

        builder.append("\n");
        builder.append(GRAY)
                .append("========== CLUSTER SNAPSHOT ")
                .append(LocalTime.now().format(TIME_FORMAT))
                .append(" ==========")
                .append(RESET)
                .append("\n");

        for (Node node : cluster.values()) {
            builder.append(formatNode(node)).append("\n");
        }

        builder.append(GRAY)
                .append("============================================")
                .append(RESET);

        System.out.println(builder);
    }

    private static String formatNode(Node node) {
        if (!node.isAlive()) {
            return RED
                    + "Node " + node.getId()
                    + " | status=DOWN"
                    + " | leader=" + node.getLeaderId()
                    + RESET;
        }

        if (node.getState() == State.LEADER) {
            return GREEN
                    + "Node " + node.getId()
                    + " | status=LEADER"
                    + " | leader=" + node.getLeaderId()
                    + RESET;
        }

        return YELLOW
                + "Node " + node.getId()
                + " | status=FOLLOWER"
                + " | leader=" + node.getLeaderId()
                + RESET;
    }
}