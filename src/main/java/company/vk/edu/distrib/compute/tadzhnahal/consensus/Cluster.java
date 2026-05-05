package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Cluster {
    private static final Logger LOG = System.getLogger(Cluster.class.getName());

    private final List<Node> nodes = new ArrayList<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<FailureSimulator> failureSimulator = new AtomicReference<>();

    public Cluster(int nodeCount) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("node count must be positive");
        }

        for (int i = 1; i <= nodeCount; i++) {
            nodes.add(new Node(i, this));
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        LogHelper.info(LOG, "cluster starts");

        for (Node node : nodes) {
            node.start();
        }

        if (!nodes.isEmpty()) {
            nodes.get(0).startElection();
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        LogHelper.info(LOG, "cluster stops");

        stopRandomFailures();

        for (Node node : nodes) {
            node.shutdown();
        }

        for (Node node : nodes) {
            waitThread(node);
        }

        LogHelper.info(LOG, "cluster stopped");
    }

    public List<Node> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public Node getNode(int nodeId) {
        for (Node node : nodes) {
            if (node.getNodeId() == nodeId) {
                return node;
            }
        }

        throw new IllegalArgumentException("unknown node id: " + nodeId);
    }

    public void turnOffNode(int nodeId) {
        getNode(nodeId).turnOff();
    }

    public void turnOnNode(int nodeId) {
        getNode(nodeId).turnOn();
    }

    public void startRandomFailures() {
        FailureSimulator currentSimulator = failureSimulator.get();

        if (currentSimulator != null && currentSimulator.isAlive()) {
            return;
        }

        FailureSimulator newSimulator = new FailureSimulator(this);
        failureSimulator.set(newSimulator);
        newSimulator.start();
    }

    public void stopRandomFailures() {
        FailureSimulator simulator = failureSimulator.get();

        if (simulator == null) {
            return;
        }

        if (!simulator.isAlive()) {
            return;
        }

        simulator.stopSimulator();
        waitThread(simulator);
    }

    public synchronized int getCurrentLeaderId() {
        for (Node node : nodes) {
            if (node.isWorking() && node.getLeaderId() == node.getNodeId()) {
                return node.getNodeId();
            }
        }

        return Node.NO_LEADER;
    }

    public void printState() {
        LogHelper.info(LOG, "cluster state:");

        for (Node node : nodes) {
            LogHelper.info(
                    LOG,
                    () -> "node " + node.getNodeId()
                            + " | " + node.getStatus()
                            + " | leader=" + node.getLeaderId()
                            + " | threadAlive=" + node.isAlive()
                            + " | inbox=" + node.getInboxSize()
            );
        }
    }

    private void waitThread(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
