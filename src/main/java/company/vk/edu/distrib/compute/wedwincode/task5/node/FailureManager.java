package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

final class FailureManager {
    private final Node node;
    private final Random random = new Random();
    private final AtomicBoolean randomFailuresEnabled = new AtomicBoolean(true);

    FailureManager(Node node) {
        this.node = node;
    }

    void setRandomFailuresEnabled(boolean value) {
        randomFailuresEnabled.set(value);
    }

    void maybeRandomCrash() {
        if (!randomFailuresEnabled.get()) {
            return;
        }

        if (!node.isManuallyEnabled()) {
            return;
        }

        if (!node.isCrashed() && random.nextDouble() < 0.002) {
            crashAndScheduleRecovery();
        }
    }

    private void crashAndScheduleRecovery() {
        node.markCrashed();
        ClusterLogger.event(node.getId(), "randomly crashed");

        new Thread(this::recoverAfterDelay, "recovery-node-" + node.getId()).start();
    }

    private void recoverAfterDelay() {
        try {
            Thread.sleep(2_000 + random.nextInt(4_000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (!node.isManuallyEnabled()) {
            return;
        }

        node.markRecovered();
        ClusterLogger.event(node.getId(), "recovered");
        node.receiveMessage(new Message(Message.Type.ELECT, node.getId()));
    }
}
