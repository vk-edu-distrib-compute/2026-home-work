package company.vk.edu.distrib.compute.wedwincode.task5;

import java.util.Map;

public class ClusterMonitor implements Runnable {
    private final Map<Integer, Node> cluster;
    private volatile boolean running = true;

    public ClusterMonitor(Map<Integer, Node> cluster) {
        this.cluster = cluster;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            ClusterLogger.clusterSnapshot(cluster);

            try {
                Thread.sleep(2_500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}