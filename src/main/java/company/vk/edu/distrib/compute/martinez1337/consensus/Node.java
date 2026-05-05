package company.vk.edu.distrib.compute.martinez1337.consensus;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Node extends Thread {
    private final int id;
    private final List<Node> allNodes;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private volatile int leaderId = -1;
    private volatile boolean alive = true;
    private final Random random = new Random();

    private volatile long lastLeaderResponse = System.currentTimeMillis();
    private static final long PING_INTERVAL = 2000;
    private static final long TIMEOUT = 4000;
    private static final long ELECTION_TIMEOUT = 1200;
    private static final double DEATH_PROBABILITY = 0.05;

    private final Object electionLock = new Object();
    private volatile boolean electionInProgress = false;
    private volatile boolean higherNodeAnswered = false;
    private volatile ScheduledFuture<?> electionTimeoutTask;
    private volatile ScheduledExecutorService scheduler;

    public Node(int id, List<Node> allNodes) {
        this.id = id;
        this.allNodes = allNodes;
    }

    @Override
    public void run() {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::sendPingIfNeeded, 0, PING_INTERVAL, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::checkLeaderTimeout, 0, 500, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::randomFailureSimulation, 0, 1000, TimeUnit.MILLISECONDS);

        startElection();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message msg = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null && alive) {
                    handleMessage(msg);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        scheduler.shutdown();
    }

    private void handleMessage(Message msg) {
        switch (msg.type()) {
            case PING -> {
                if (leaderId == id && alive) {
                    sendTo(msg.senderId(), new Message(MessageType.ANSWER, id));
                }
            }
            case ELECT -> {
                if (!alive) {
                    return;
                }
                if (msg.senderId() < id) {
                    sendTo(msg.senderId(), new Message(MessageType.ANSWER, id));
                    startElection();
                }
            }
            case ANSWER -> {
                if (msg.senderId() == leaderId) {
                    lastLeaderResponse = System.currentTimeMillis();
                }
                if (msg.senderId() > id) {
                    higherNodeAnswered = true;
                }
            }
            case VICTORY -> {
                leaderId = msg.senderId();
                lastLeaderResponse = System.currentTimeMillis();
                synchronized (electionLock) {
                    electionInProgress = false;
                    higherNodeAnswered = false;
                    if (electionTimeoutTask != null) {
                        electionTimeoutTask.cancel(false);
                        electionTimeoutTask = null;
                    }
                }
            }
        }
    }

    private void sendPingIfNeeded() {
        if (!alive) return;
        if (leaderId == -1) {
            startElection();
            return;
        }
        if (leaderId != -1 && leaderId != id) {
            Node leader = getNodeById(leaderId);
            if (leader != null) {
                leader.send(new Message(MessageType.PING, id));
            }
        }
    }

    private void checkLeaderTimeout() {
        if (!alive) return;
        if (leaderId != -1 && leaderId != id && System.currentTimeMillis() - lastLeaderResponse > TIMEOUT) {
            startElection();
        }
    }

    synchronized void startElection() {
        if (!alive) return;
        synchronized (electionLock) {
            if (electionInProgress) {
                return;
            }
            electionInProgress = true;
            higherNodeAnswered = false;
            if (electionTimeoutTask != null) {
                electionTimeoutTask.cancel(false);
                electionTimeoutTask = null;
            }
        }

        for (Node node : allNodes) {
            if (node.id > id) {
                node.send(new Message(MessageType.ELECT, id));
            }
        }

        electionTimeoutTask = scheduler.schedule(() -> {
            if (!alive) return;
            synchronized (electionLock) {
                if (!electionInProgress) return;
                if (!higherNodeAnswered) {
                    declareVictory();
                }
                electionInProgress = false;
                higherNodeAnswered = false;
                electionTimeoutTask = null;
            }
        }, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void declareVictory() {
        leaderId = id;
        allNodes.forEach(node -> node.send(new Message(MessageType.VICTORY, id)));
    }

    public void crash() {
        alive = false;
        synchronized (electionLock) {
            electionInProgress = false;
            higherNodeAnswered = false;
            if (electionTimeoutTask != null) {
                electionTimeoutTask.cancel(false);
                electionTimeoutTask = null;
            }
        }
    }

    public void recover() {
        alive = true;
        leaderId = -1;
        lastLeaderResponse = System.currentTimeMillis();
        startElection();
    }

    private void randomFailureSimulation() {
        if (random.nextDouble() < DEATH_PROBABILITY) {
            crash();
            new Thread(() -> {
                try {
                    Thread.sleep(3000 + random.nextInt(5000));
                } catch (InterruptedException ignored) {}
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

    public int getNodeId() { return id; }

    public int getLeaderId() { return leaderId; }

    public boolean isUp() { return alive; }
}
