package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

final class Election {
    private static final Logger LOG = System.getLogger(Election.class.getName());

    private static final long ANSWER_TIMEOUT_MS = 500L;
    private static final long VICTORY_TIMEOUT_MS = 1000L;

    private final Node node;

    Election(Node node) {
        this.node = node;
    }

    void start() {
        Thread electionThread = new Thread(this::run, node.getName() + "-election");
        electionThread.start();
    }

    private void run() {
        LogHelper.info(LOG, () -> node.getName() + " starts election");

        node.sendElectToHigherNodes();
        sleep(ANSWER_TIMEOUT_MS);

        if (!node.isWorking()) {
            node.finishElection();
            return;
        }

        if (!node.hasAnswerFromHigher()) {
            becomeLeader();
            return;
        }

        LogHelper.info(LOG, () -> node.getName() + " waits for victory from higher node");

        int leaderBeforeWait = node.getLeaderId();
        sleep(VICTORY_TIMEOUT_MS);

        if (!node.isWorking()) {
            node.finishElection();
            return;
        }

        if (node.getLeaderId() == leaderBeforeWait || node.getLeaderId() == Node.NO_LEADER) {
            node.finishElection();
            node.clearLeader();
            node.startElection();
        } else {
            node.finishElection();
        }
    }

    private void becomeLeader() {
        node.becomeLeader();

        LogHelper.info(LOG, () -> node.getName() + " becomes leader");

        node.sendVictoryToAll();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
