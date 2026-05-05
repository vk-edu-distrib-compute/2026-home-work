package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class ClusterStatePrinter implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ClusterStatePrinter.class.getName());

    private final Cluster cluster;
    private final long pauseMillis;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread thread;

    public ClusterStatePrinter(Cluster cluster, long pauseMillis) {
        this.cluster = cluster;
        this.pauseMillis = pauseMillis;
    }

    public void start() {
        if (running.get()) {
            return;
        }

        running.set(true);
        thread = new Thread(this, "cluster-state-printer");
        thread.start();
    }

    public void stop() {
        running.set(false);

        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void run() {
        LOGGER.info("cluster state printer started");

        while (running.get()) {
            cluster.printState();
            sleep();
        }

        LOGGER.info("cluster state printer stopped");
    }

    private void sleep() {
        try {
            Thread.sleep(pauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
