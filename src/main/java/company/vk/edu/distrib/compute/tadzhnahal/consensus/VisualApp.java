package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.util.logging.Logger;

public final class VisualApp {
    private static final Logger LOGGER = Logger.getLogger(VisualApp.class.getName());

    private VisualApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        Cluster cluster = new Cluster(5);
        ClusterStatePrinter printer = new ClusterStatePrinter(cluster, 1000);

        cluster.start();
        printer.start();

        Thread.sleep(3000);

        LOGGER.info("visual demo: turn off node 5");
        cluster.turnOffNode(5);

        Thread.sleep(4000);

        LOGGER.info("visual demo: turn on node 5");
        cluster.turnOnNode(5);

        Thread.sleep(4000);

        LOGGER.info("visual demo: stop");
        printer.stop();
        cluster.stop();
    }
}
