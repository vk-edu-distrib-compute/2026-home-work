package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

final class FailureSimulator extends Thread {
    private static final Logger LOG = System.getLogger(FailureSimulator.class.getName());

    private static final int FAILURE_PERCENT = 25;
    private static final int RECOVERY_PERCENT = 35;
    private static final long STEP_PAUSE_MS = 1000L;

    private final Cluster cluster;
    private final Random random = new Random();
    private final AtomicBoolean running = new AtomicBoolean(true);

    FailureSimulator(Cluster cluster) {
        super("failure-simulator");
        this.cluster = cluster;
    }

    void stopSimulator() {
        running.set(false);
        interrupt();
    }

    @Override
    public void run() {
        LogHelper.info(LOG, "failure simulator started");

        while (running.get()) {
            try {
                makeRandomStep();
                sleep(STEP_PAUSE_MS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                break;
            }
        }

        LogHelper.info(LOG, "failure simulator stopped");
    }

    private void makeRandomStep() {
        maybeTurnOff();
        maybeTurnOn();
    }

    private void maybeTurnOff() {
        if (random.nextInt(100) >= FAILURE_PERCENT) {
            return;
        }

        List<Node> workingNodes = new ArrayList<>();
        for (Node node : cluster.getNodes()) {
            if (node.isWorking()) {
                workingNodes.add(node);
            }
        }

        if (workingNodes.size() <= 1) {
            return;
        }

        Node node = workingNodes.get(random.nextInt(workingNodes.size()));

        LogHelper.info(
                LOG,
                () -> "random failure: turn off node " + node.getNodeId()
        );

        cluster.turnOffNode(node.getNodeId());
    }

    private void maybeTurnOn() {
        if (random.nextInt(100) >= RECOVERY_PERCENT) {
            return;
        }

        List<Node> downNodes = new ArrayList<>();
        for (Node node : cluster.getNodes()) {
            if (!node.isWorking()) {
                downNodes.add(node);
            }
        }

        if (downNodes.isEmpty()) {
            return;
        }

        Node node = downNodes.get(random.nextInt(downNodes.size()));

        LogHelper.info(
                LOG,
                () -> "random recovery: turn on node " + node.getNodeId()
        );

        cluster.turnOnNode(node.getNodeId());
    }
}
