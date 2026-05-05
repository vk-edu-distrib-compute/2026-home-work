package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node extends Thread {

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    static final double FAILURE_PROB_PER_TICK = 0.002;
    static final int MIN_RECOVERY_MS = 2000;
    static final int MAX_RECOVERY_MS = 5000;
    private static final int TICK_MS = 100;

    private final int id;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ElectionCoordinator coordinator;
    private final Random random = new Random();

    private boolean randomFailuresEnabled;

    public Node(int id) {
        super("node-" + id);
        this.id = id;
        this.coordinator = new ElectionCoordinator(id);
    }

    public void setPeers(List<Node> peers) {
        coordinator.setPeers(peers);
    }

    public void setRandomFailuresEnabled(boolean enabled) {
        this.randomFailuresEnabled = enabled;
    }

    public void deliver(Message msg) {
        if (!coordinator.isDown()) {
            inbox.offer(msg);
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                Message msg = inbox.poll(TICK_MS, TimeUnit.MILLISECONDS);
                process(msg);
            } catch (InterruptedException ex) {
                currentThread().interrupt();
                break;
            }
        }
    }

    private synchronized void process(Message msg) {
        if (coordinator.isDown()) {
            return;
        }
        if (randomFailuresEnabled && random.nextDouble() < FAILURE_PROB_PER_TICK) {
            simulateCrash();
            return;
        }
        if (msg != null) {
            coordinator.handleMessage(msg);
        }
        coordinator.onTick();
    }

    private void simulateCrash() {
        log.info("Node {}: simulating crash (random failure)", id);
        coordinator.markDown();
        inbox.clear();
        int delay = MIN_RECOVERY_MS + random.nextInt(MAX_RECOVERY_MS - MIN_RECOVERY_MS);
        Thread recovery = new Thread(() -> {
            try {
                sleep(delay);
                forceUp();
            } catch (InterruptedException ex) {
                currentThread().interrupt();
            }
        }, "recovery-" + id);
        recovery.setDaemon(true);
        recovery.start();
    }

    public synchronized void forceDown() {
        if (coordinator.markDown()) {
            log.info("Node {}: forced DOWN", id);
            inbox.clear();
        }
    }

    public synchronized void forceUp() {
        if (coordinator.recover()) {
            log.info("Node {}: forced UP (recovering)", id);
            inbox.clear();
        }
    }

    public void shutdown() {
        running.set(false);
        interrupt();
    }

    public int getNodeId() {
        return id;
    }

    public NodeState getNodeState() {
        return coordinator.getCurrentState();
    }

    public synchronized int getLeaderId() {
        return coordinator.getLeaderId();
    }
}
