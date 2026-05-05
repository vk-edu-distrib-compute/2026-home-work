package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.TimeUnit;

final class Election {
    private static final Logger LOG = System.getLogger(Election.class.getName());

    private static final long ELECTION_WAIT_MS = 400L;

    private final Node node;

    Election(Node node) {
        this.node = node;
    }

    void start() {
        Thread electionThread = new Thread(this::run, node.getName() + "-election");
        electionThread.start();
    }

    private void run() {
        if (!node.tryStartElection()) {
            return;
        }

        try {
            LOG.log(Logger.Level.INFO, node.getName() + " starts election");

            int electCount = sendElectToHigherNodes();

            if (electCount == 0) {
                becomeLeader();
                return;
            }

            waitForAnswers();

            if (!node.hasAnswerFromHigherNode()) {
                becomeLeader();
                return;
            }

            LOG.log(Logger.Level.INFO, node.getName() + " waits for victory from higher node");
        } finally {
            node.finishElection();
        }
    }

    private int sendElectToHigherNodes() {
        int electCount = 0;

        for (Node otherNode : node.getClusterNodesSnapshot()) {
            if (otherNode.getNodeId() > node.getNodeId()) {
                node.sendMessage(otherNode.getNodeId(), MessageType.ELECT);
                electCount++;
            }
        }

        return electCount;
    }

    private void waitForAnswers() {
        try {
            TimeUnit.MILLISECONDS.sleep(ELECTION_WAIT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void becomeLeader() {
        node.rememberLeader(node.getNodeId());

        LOG.log(Logger.Level.INFO, node.getName() + " becomes leader");

        for (Node otherNode : node.getClusterNodesSnapshot()) {
            if (otherNode.getNodeId() != node.getNodeId()) {
                node.sendMessage(otherNode.getNodeId(), MessageType.VICTORY, node.getNodeId());
            }
        }
    }
}
