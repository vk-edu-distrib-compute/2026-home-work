package company.vk.edu.distrib.compute.wedwincode.task5;

import company.vk.edu.distrib.compute.wedwincode.task5.node.Node;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterMonitor implements Runnable {
    private final Map<Integer, Node> cluster;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ClusterMonitor(Map<Integer, Node> cluster) {
        this.cluster = cluster;
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        while (running.get()) {
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
