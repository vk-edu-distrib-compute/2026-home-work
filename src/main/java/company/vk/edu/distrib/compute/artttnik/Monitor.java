package company.vk.edu.distrib.compute.artttnik;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.logging.Level;

public class Monitor implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(Monitor.class.getName());

    private final Cluster cluster;
    private final long intervalMs;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public Monitor(Cluster cluster, long intervalMs) {
        this.cluster = cluster;
        this.intervalMs = intervalMs;
    }

    public void stop() {
        running.set(false);
    }

    private String color(String s, String colorCode) {
        return "\u001B[" + colorCode + "m" + s + "\u001B[0m";
    }

    @Override
    public void run() {
        while (running.get()) {
            if (LOGGER.isLoggable(Level.INFO)) {
                StringBuilder sb = new StringBuilder(128);
                sb.append("---- CLUSTER STATUS ----\n");
                for (int id : cluster.nodeIds()) {
                    sb.append(formatNodeLine(id)).append('\n');
                }
                LOGGER.info(sb.toString());
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private String formatNodeLine(int id) {
        Node n = cluster.getNode(id);
        boolean alive = n.isAliveNode();
        Integer leader = n.getLeaderId();
        String status = alive ? "UP" : "DOWN";
        String leaderText = leader == null ? "-" : leader.toString();
        String line = String.format("Node %2d: %s, leader=%s", id, status, leaderText);
        if (!alive) {
            return color(line, "31");
        }
        if (leader != null && leader == id) {
            return color(line, "32");
        }
        return line;
    }
}
