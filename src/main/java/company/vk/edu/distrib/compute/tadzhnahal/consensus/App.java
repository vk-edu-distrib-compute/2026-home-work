package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

public final class App {
    private static final Logger LOG = System.getLogger(App.class.getName());

    private static final int NODE_COUNT = 5;
    private static final int FIRST_LEADER_ID = 5;
    private static final String CURRENT_LEADER_MESSAGE = "demo: current leader is node ";

    private App() {
    }

    public static void main(String[] args) {
        Cluster cluster = new Cluster(NODE_COUNT);

        try {
            cluster.start();
            sleep(2000L);
            cluster.printState();
            logCurrentLeader(cluster);

            LogHelper.info(LOG, () -> "demo: turn off node " + FIRST_LEADER_ID);
            cluster.turnOffNode(FIRST_LEADER_ID);
            sleep(3000L);
            cluster.printState();
            logCurrentLeader(cluster);

            LogHelper.info(LOG, () -> "demo: turn on node " + FIRST_LEADER_ID);
            cluster.turnOnNode(FIRST_LEADER_ID);
            sleep(2000L);
            cluster.printState();
            logCurrentLeader(cluster);

            LogHelper.info(LOG, "demo: start random failures");
            cluster.startRandomFailures();
            sleep(8000L);

            LogHelper.info(LOG, "demo: stop random failures");
            cluster.stopRandomFailures();
            sleep(1000L);
            cluster.printState();
            logCurrentLeader(cluster);
        } finally {
            cluster.stop();
        }
    }

    private static void logCurrentLeader(Cluster cluster) {
        LogHelper.info(LOG, () -> CURRENT_LEADER_MESSAGE + cluster.getCurrentLeaderId());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
