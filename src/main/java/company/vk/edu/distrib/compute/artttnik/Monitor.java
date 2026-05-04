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

    private String formatNodeLine(int id, Node node) {
        boolean alive = node.isAliveNode();
        Integer leader = node.getLeaderId();
        String status = alive ? "UP" : "DOWN";
        String leaderText = leader == null ? "-" : leader.toString();
        String line = String.format("Node %2d: %s, leader=%s", id, status, leaderText);
        
        if (alive && leader != null && leader == id) {
            return color(line, "32");
        } else if (!alive) {
            return color(line, "31");
        }
        return line;
    }

    private void buildAndLogClusterStatus() {
        if (!LOGGER.isLoggable(Level.INFO)) {
            return;
        }

        StringBuilder sb = new StringBuilder(128);
        sb.append("---- CLUSTER STATUS ----\n");
        for (int id : cluster.nodeIds()) {
            sb.append(formatNodeLine(id, cluster.getNode(id))).append('\n');
        }
        LOGGER.info(sb.toString());
    }

    @Override
    public void run() {
        while (running.get()) {
            if (LOGGER.isLoggable(Level.INFO)) {
                buildAndLogClusterStatus();
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
