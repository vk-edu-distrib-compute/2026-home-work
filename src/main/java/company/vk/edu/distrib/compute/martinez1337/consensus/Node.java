package company.vk.edu.distrib.compute.martinez1337.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class Node extends Thread {
    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private final int id;
    private final List<Node> allNodes;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final Random random = new Random();

    private final AtomicLong lastLeaderResponse = new AtomicLong(System.currentTimeMillis());
    private static final long PING_INTERVAL = 2000;
    private static final long TIMEOUT = 4000;
    private static final long ELECTION_TIMEOUT = 1200;
    private static final double DEATH_PROBABILITY = 0.05;

    private final ReentrantLock electionLock = new ReentrantLock();
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean higherNodeAnswered = new AtomicBoolean(false);
    private final AtomicReference<ScheduledFuture<?>> electionTimeoutTask = new AtomicReference<>();
    private final AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<>();

    public Node(int id, List<Node> allNodes) {
        super();
        this.id = id;
        this.allNodes = allNodes;
    }

    @Override
    public void run() {
        scheduler.set(Executors.newScheduledThreadPool(1));
        scheduler.get().scheduleAtFixedRate(this::sendPingIfNeeded, 0, PING_INTERVAL, TimeUnit.MILLISECONDS);
        scheduler.get().scheduleAtFixedRate(this::checkLeaderTimeout, 0, 500, TimeUnit.MILLISECONDS);
        scheduler.get().scheduleAtFixedRate(this::randomFailureSimulation, 0, 1000, TimeUnit.MILLISECONDS);

        startElection();

        while (!currentThread().isInterrupted()) {
            try {
                Message msg = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null && alive.get()) {
                    handleMessage(msg);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        ScheduledExecutorService s = scheduler.get();
        if (s != null) {
            s.shutdown();
        }
    }

    private void handleMessage(Message msg) {
        switch (msg.type()) {
            case PING -> {
                if (leaderId.get() == id && alive.get()) {
                    sendTo(msg.senderId(), new Message(MessageType.ANSWER, id));
                }
            }
            case ELECT -> {
                if (!alive.get()) {
                    return;
                }
                if (msg.senderId() < id) {
                    sendTo(msg.senderId(), new Message(MessageType.ANSWER, id));
                    startElection();
                }
            }
            case ANSWER -> {
                if (msg.senderId() == leaderId.get()) {
                    lastLeaderResponse.set(System.currentTimeMillis());
                }
                if (msg.senderId() > id) {
                    higherNodeAnswered.set(true);
                }
            }
            case VICTORY -> {
                leaderId.set(msg.senderId());
                lastLeaderResponse.set(System.currentTimeMillis());
                resetElectionState();
            }
        }
    }

    private void sendPingIfNeeded() {
        if (!alive.get()) {
            return;
        }
        if (leaderId.get() == -1) {
            startElection();
            return;
        }
        if (leaderId.get() != -1 && leaderId.get() != id) {
            Node leader = getNodeById(leaderId.get());
            if (leader != null) {
                leader.send(new Message(MessageType.PING, id));
            }
        }
    }

    private void checkLeaderTimeout() {
        if (!alive.get()) {
            return;
        }
        if (leaderId.get() != -1
                && leaderId.get() != id
                && System.currentTimeMillis() - lastLeaderResponse.get() > TIMEOUT) {
            startElection();
        }
    }

    void startElection() {
        if (!alive.get()) {
            return;
        }
        electionLock.lock();
        try {
            if (electionInProgress.get()) {
                return;
            }
            electionInProgress.set(true);
            higherNodeAnswered.set(false);
            if (electionTimeoutTask.get() != null) {
                electionTimeoutTask.get().cancel(false);
                electionTimeoutTask.set(null);
            }
        } finally {
            electionLock.unlock();
        }

        for (Node node : allNodes) {
            if (node.id > id) {
                node.send(new Message(MessageType.ELECT, id));
            }
        }

        electionTimeoutTask.set(scheduler.get().schedule(() -> {
            if (!alive.get()) {
                return;
            }
            electionLock.lock();
            try {
                if (!electionInProgress.get()) {
                    return;
                }
                if (!higherNodeAnswered.get()) {
                    declareVictory();
                }
                electionInProgress.set(false);
                higherNodeAnswered.set(false);
                electionTimeoutTask.set(null);
            } finally {
                electionLock.unlock();
            }
        }, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    private void declareVictory() {
        leaderId.set(id);
        allNodes.forEach(node -> node.send(new Message(MessageType.VICTORY, id)));
    }

    public void crash() {
        alive.set(false);
        resetElectionState();
    }

    private void resetElectionState() {
        electionLock.lock();
        try {
            electionInProgress.set(false);
            higherNodeAnswered.set(false);
            if (electionTimeoutTask.get() != null) {
                electionTimeoutTask.get().cancel(false);
                electionTimeoutTask.set(null);
            }
        } finally {
            electionLock.unlock();
        }
    }

    public void recover() {
        alive.set(true);
        leaderId.set(-1);
        lastLeaderResponse.set(System.currentTimeMillis());
        startElection();
    }

    private void randomFailureSimulation() {
        if (random.nextDouble() < DEATH_PROBABILITY) {
            crash();
            new Thread(() -> {
                try {
                    sleep(3000 + random.nextInt(5000));
                } catch (InterruptedException ignored) {
                    log.warn("sleep interrupted");
                }
                recover();
            }).start();
        }
    }

    public void send(Message msg) {
        inbox.offer(msg);
    }

    private void sendTo(int nodeId, Message msg) {
        Node node = getNodeById(nodeId);
        if (node != null) {
            node.send(msg);
        }
    }

    private Node getNodeById(int nodeId) {
        for (Node node : allNodes) {
            if (node.id == nodeId) {
                return node;
            }
        }
        return null;
    }

    public int getNodeId() {
        return id;
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public boolean isUp() {
        return alive.get();
    }
}
