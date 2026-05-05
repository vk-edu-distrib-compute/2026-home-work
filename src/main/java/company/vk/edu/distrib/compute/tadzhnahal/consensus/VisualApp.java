package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

public final class VisualApp {
    private static final Logger LOG = System.getLogger(VisualApp.class.getName());

    private static final int NODE_COUNT = 5;
    private static final int LEADER_ID = 5;
    private static final long PRINT_PAUSE_MS = 1000L;

    private VisualApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        Cluster cluster = new Cluster(NODE_COUNT);
        ClusterStatePrinter printer = new ClusterStatePrinter(cluster, PRINT_PAUSE_MS);

        try {
            cluster.start();
            printer.start();

            Thread.sleep(3000L);

            LogHelper.info(LOG, () -> "visual demo: turn off node " + LEADER_ID);
            cluster.turnOffNode(LEADER_ID);

            Thread.sleep(4000L);

            LogHelper.info(LOG, () -> "visual demo: turn on node " + LEADER_ID);
            cluster.turnOnNode(LEADER_ID);

            Thread.sleep(4000L);

            LogHelper.info(LOG, "visual demo: stop");
        } finally {
            printer.stop();
            cluster.stop();
        }
    }
}
