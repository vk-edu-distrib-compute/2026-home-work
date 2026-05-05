package company.vk.edu.distrib.compute.wedwincode.task5;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Node implements Runnable {
    private static final long PING_INTERVAL_MS = 1_000;
    private static final long ANSWER_TIMEOUT_MS = 700;
    private static final long ELECTION_TIMEOUT_MS = 1_500;

    private final int id;
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private final Random random = new Random();

    private Map<Integer, Node> nodes = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private volatile boolean manuallyEnabled = true;
    private volatile boolean crashed = false;

    private volatile State state = State.FOLLOWER;
    private volatile int leaderId = -1;

    private volatile long lastPingTime = 0;
    private volatile long lastAnswerFromLeaderTime = 0;

    private volatile boolean electionInProgress = false;

    public Node(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public boolean isAlive() {
        return manuallyEnabled && !crashed;
    }

    public void setCluster(Map<Integer, Node> nodes) {
        this.nodes = nodes;
    }

    public void setEnabled(boolean value) {
        manuallyEnabled = value;

        if (value) {
            ClusterLogger.event(id, "was manually enabled");
            receiveMessage(new Message(Message.Type.ELECT, id));
        } else {
            ClusterLogger.event(id, "was manually disabled");
            state = State.FOLLOWER;
            leaderId = -1;
            messages.clear();
        }
    }

    public void stop() {
        running = false;
    }


    private boolean sendMessage(Node node, Message message) {
        if (!isAlive()) {
            return false;
        }

        return node.receiveMessage(message);
    }

    public boolean receiveMessage(Message message) {
        if (!isAlive()) {
            return false;
        }

        messages.offer(message);
        return true;
    }

    private void handleMessage(Message message) {
        if (!isAlive()) {
            return;
        }

        switch (message.type()) {
            case PING -> handlePing(message);
            case ELECT -> handleElect(message);
            case ANSWER -> handleAnswer(message);
            case VICTORY -> handleVictory(message);
            default -> throw new IllegalArgumentException("Incorrect message type");
        }
    }

    private void handlePing(Message message) {
        Node sender = nodes.get(message.senderId());

        if (sender != null) {
            sendMessage(sender, new Message(Message.Type.ANSWER, id));
        }
    }

    private void handleElect(Message message) {
        int senderId = message.senderId();
        Node sender = nodes.get(senderId);

        if (sender != null && senderId < id) {
            sendMessage(sender, new Message(Message.Type.ANSWER, id));
        }

        if (state == State.LEADER) {
            return;
        }

        Node currentLeader = nodes.get(leaderId);
        if (leaderId != -1 && currentLeader != null && currentLeader.isAlive()) {
            return;
        }

        if (senderId < id && !electionInProgress) {
            startElection();
        }
    }

    private void handleAnswer(Message message) {
        if (message.senderId() == leaderId) {
            lastAnswerFromLeaderTime = System.currentTimeMillis();
        }
    }

    private void handleVictory(Message message) {
        int newLeaderId = message.senderId();

        if (newLeaderId > id) {
            leaderId = newLeaderId;
            state = State.FOLLOWER;
            electionInProgress = false;

            lastAnswerFromLeaderTime = System.currentTimeMillis();
            lastPingTime = 0;

            ClusterLogger.event(id, "accepted leader " + leaderId);
            return;
        }

        if (newLeaderId < id && isAlive()) {
            startElection();
            return;
        }

        if (newLeaderId == id) {
            leaderId = id;
            state = State.LEADER;
            electionInProgress = false;

            lastAnswerFromLeaderTime = System.currentTimeMillis();
            lastPingTime = 0;
        }
    }

    private synchronized void startElection() {
        if (!isAlive() || electionInProgress) {
            return;
        }

        electionInProgress = true;
        state = State.FOLLOWER;
        leaderId = -1;

        ClusterLogger.event(id, "started election");

        boolean hasAnswerFromHigherNode = false;

        Message elect = new Message(Message.Type.ELECT, id);

        for (Node node : nodes.values()) {
            if (node.getId() > id) {
                boolean delivered = sendMessage(node, elect);

                if (!delivered) {
                    ClusterLogger.event(id, "could not reach higher node " + node.getId());
                }
            }
        }

        long deadline = System.currentTimeMillis() + ANSWER_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();

            try {
                Message message = messages.poll(remaining, TimeUnit.MILLISECONDS);

                if (message == null) {
                    break;
                }

                if (message.type() == Message.Type.ANSWER && message.senderId() > id) {
                    hasAnswerFromHigherNode = true;
                    ClusterLogger.event(id, "got ANSWER from higher node " + message.senderId());
                    continue;
                }

                if (message.type() == Message.Type.VICTORY && message.senderId() > id) {
                    handleVictory(message);
                    return;
                }

                handleMessage(message);

                if (leaderId != -1 && !electionInProgress) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (!hasAnswerFromHigherNode) {
            becomeLeader();
            return;
        }

        long victoryDeadline = System.currentTimeMillis() + ELECTION_TIMEOUT_MS;

        while (System.currentTimeMillis() < victoryDeadline) {
            long remaining = victoryDeadline - System.currentTimeMillis();

            try {
                Message message = messages.poll(remaining, TimeUnit.MILLISECONDS);

                if (message == null) {
                    break;
                }

                if (message.type() == Message.Type.VICTORY && message.senderId() > id) {
                    handleVictory(message);
                    return;
                }

                handleMessage(message);

                if (leaderId != -1 && !electionInProgress) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (leaderId != -1) {
            electionInProgress = false;
            return;
        }

        electionInProgress = false;
        receiveMessage(new Message(Message.Type.ELECT, id));
    }

    private void becomeLeader() {
        leaderId = id;
        state = State.LEADER;
        electionInProgress = false;

        lastAnswerFromLeaderTime = System.currentTimeMillis();
        lastPingTime = 0;

        ClusterLogger.event(id, "became LEADER");

        Message victory = new Message(Message.Type.VICTORY, id);

        for (Node node : nodes.values()) {
            if (node.getId() != id) {
                sendMessage(node, victory);
            }
        }
    }

    private void checkLeader() {
        if (!isAlive()) {
            return;
        }

        if (leaderId == -1 && !electionInProgress) {
            startElection();
            return;
        }

        if (state == State.LEADER) {
            return;
        }

        Node leader = nodes.get(leaderId);

        if (leader == null) {
            startElection();
            return;
        }

        long now = System.currentTimeMillis();

        if (now - lastPingTime >= PING_INTERVAL_MS) {
            lastPingTime = now;

            boolean delivered = sendMessage(leader, new Message(Message.Type.PING, id));
            if (!delivered) {
                ClusterLogger.event(id, " detected unavailable leader " + leaderId);
                startElection();
            }
        }

        if (lastPingTime > 0 &&
                lastAnswerFromLeaderTime != 0 &&
                now - lastAnswerFromLeaderTime > PING_INTERVAL_MS + ANSWER_TIMEOUT_MS * 2) {
            ClusterLogger.event(id, " timed out leader " + leaderId);
            startElection();
        }
    }

    private void maybeRandomCrash() {
        if (!manuallyEnabled) {
            return;
        }

        if (!crashed && random.nextDouble() < 0.002) {
            crashed = true;
            state = State.FOLLOWER;
            leaderId = -1;
            messages.clear();

            ClusterLogger.event(id, " randomly crashed");

            new Thread(() -> {
                try {
                    Thread.sleep(2_000 + random.nextInt(4_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                crashed = false;
                ClusterLogger.event(id, " recovered");

                receiveMessage(new Message(Message.Type.ELECT, id));
            }).start();
        }
    }

    public synchronized void gracefulShutdown() {
        if (!isAlive()) {
            return;
        }

        ClusterLogger.event(id, "requested graceful shutdown");

        if (state != State.LEADER) {
            ClusterLogger.event(id, "is not leader, shutting down normally");
            setEnabled(false);
            return;
        }

        Node nextLeader = findHighestAliveNodeExceptSelf();

        if (nextLeader == null) {
            ClusterLogger.event(id, "no alive replacement leader found, shutting down");
            setEnabled(false);
            return;
        }

        ClusterLogger.event(id, "transferring leadership to node " + nextLeader.getId());

        nextLeader.forceBecomeLeaderAfterTransfer();

        Message victory = new Message(Message.Type.VICTORY, nextLeader.getId());

        for (Node node : nodes.values()) {
            if (node.getId() != id && node.getId() != nextLeader.getId()) {
                sendMessage(node, victory);
            }
        }

        setEnabled(false);
    }

    private Node findHighestAliveNodeExceptSelf() {
        Node result = null;

        for (Node node : nodes.values()) {
            if (node.getId() == id) {
                continue;
            }

            if (!node.isAlive()) {
                continue;
            }

            if (result == null || node.getId() > result.getId()) {
                result = node;
            }
        }

        return result;
    }

    private synchronized void forceBecomeLeaderAfterTransfer() {
        if (!isAlive()) {
            return;
        }

        leaderId = id;
        state = State.LEADER;
        electionInProgress = false;

        lastAnswerFromLeaderTime = System.currentTimeMillis();
        lastPingTime = 0;

        ClusterLogger.event(id, "became LEADER by graceful transfer");
    }

    @Override
    public void run() {
        receiveMessage(new Message(Message.Type.ELECT, id));

        while (running) {
            try {
                maybeRandomCrash();

                if (!isAlive()) {
                    Thread.sleep(200);
                    continue;
                }

                Message message = messages.poll(200, TimeUnit.MILLISECONDS);

                if (message != null) {
                    handleMessage(message);
                }

                checkLeader();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
