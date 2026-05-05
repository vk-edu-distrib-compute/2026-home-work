package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

final class FailureSimulator extends Thread {
    private static final Logger LOG = System.getLogger(FailureSimulator.class.getName());

    private static final int PERCENT_LIMIT = 100;
    private static final int FAIL_CHANCE_PERCENT = 15;
    private static final int RECOVER_CHANCE_PERCENT = 25;
    private static final int MIN_WORKING_NODES = 1;
    private static final long TICK_MS = 800L;

    private final Cluster cluster;
    private final Random random = new Random();

    FailureSimulator(Cluster cluster) {
        super("failure-simulator");

        if (cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }

        this.cluster = cluster;
    }

    @Override
    public void run() {
        LOG.log(Logger.Level.INFO, "failure simulator started");

        while (!isInterrupted()) {
            try {
                TimeUnit.MILLISECONDS.sleep(TICK_MS);
                makeRandomStep();
            } catch (InterruptedException e) {
                interrupt();
            }
        }

        LOG.log(Logger.Level.INFO, "failure simulator stopped");
    }

    private void makeRandomStep() {
        List<Node> nodes = cluster.getNodes();

        if (nodes.isEmpty()) {
            return;
        }

        Node node = nodes.get(random.nextInt(nodes.size()));

        if (node.isWorking()) {
            maybeTurnOff(node);
            return;
        }

        maybeTurnOn(node);
    }

    private void maybeTurnOff(Node node) {
        if (cluster.getWorkingNodeCount() <= MIN_WORKING_NODES) {
            return;
        }

        if (random.nextInt(PERCENT_LIMIT) < FAIL_CHANCE_PERCENT) {
            LOG.log(Logger.Level.INFO, "random failure: turn off node " + node.getNodeId());
            cluster.turnOffNode(node.getNodeId());
        }
    }

    private void maybeTurnOn(Node node) {
        if (random.nextInt(PERCENT_LIMIT) < RECOVER_CHANCE_PERCENT) {
            LOG.log(Logger.Level.INFO, "random recovery: turn on node " + node.getNodeId());
            cluster.turnOnNode(node.getNodeId());
        }
    }
}
