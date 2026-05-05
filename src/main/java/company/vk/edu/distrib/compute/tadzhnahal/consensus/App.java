package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.TimeUnit;

public final class App {
    private static final Logger LOG = System.getLogger(App.class.getName());

    private static final int DEFAULT_NODE_COUNT = 5;
    private static final int FIRST_LEADER_ID = 5;
    private static final long ELECTION_DEMO_DELAY_MS = 1500L;
    private static final long LEADER_FAIL_DEMO_DELAY_MS = 3000L;

    private App() {
    }

    public static void main(String[] args) {
        Cluster cluster = new Cluster(DEFAULT_NODE_COUNT);

        try {
            cluster.start();

            TimeUnit.MILLISECONDS.sleep(ELECTION_DEMO_DELAY_MS);

            cluster.printState();
            LOG.log(Logger.Level.INFO, "demo: current leader is node " + cluster.getLeaderId());

            LOG.log(Logger.Level.INFO, "demo: turn off node " + FIRST_LEADER_ID);
            cluster.turnOffNode(FIRST_LEADER_ID);

            TimeUnit.MILLISECONDS.sleep(LEADER_FAIL_DEMO_DELAY_MS);

            cluster.printState();
            LOG.log(Logger.Level.INFO, "demo: current leader is node " + cluster.getLeaderId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.log(Logger.Level.WARNING, "demo interrupted");
        } finally {
            cluster.stop();
        }
    }
}
