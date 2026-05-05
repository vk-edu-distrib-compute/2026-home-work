package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.atomic.AtomicBoolean;

final class ClusterStatePrinter implements Runnable {
    private static final Logger LOG = System.getLogger(ClusterStatePrinter.class.getName());

    private final Cluster cluster;
    private final long pauseMillis;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread thread;

    ClusterStatePrinter(Cluster cluster, long pauseMillis) {
        this.cluster = cluster;
        this.pauseMillis = pauseMillis;
    }

    void start() {
        if (running.compareAndSet(false, true)) {
            thread = new Thread(this, "cluster-state-printer");
            thread.start();
        }
    }

    void stop() {
        running.set(false);

        if (thread != null) {
            thread.interrupt();

            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void run() {
        LogHelper.info(LOG, "cluster state printer started");

        while (running.get()) {
            cluster.printState();

            try {
                Thread.sleep(pauseMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LogHelper.info(LOG, "cluster state printer stopped");
    }
}
