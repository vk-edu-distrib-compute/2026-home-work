package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.TimeUnit;

public final class App {
    private static final Logger LOG = System.getLogger(App.class.getName());

    private static final int DEFAULT_NODE_COUNT = 5;
    private static final long DEMO_DELAY_MS = 500L;

    private App() {
    }

    public static void main(String[] args) {
        Cluster cluster = new Cluster(DEFAULT_NODE_COUNT);

        try {
            cluster.start();
            cluster.printState();

            TimeUnit.MILLISECONDS.sleep(DEMO_DELAY_MS);

            cluster.printState();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.log(Logger.Level.WARNING, "demo interrupted");
        } finally {
            cluster.stop();
        }
    }
}
