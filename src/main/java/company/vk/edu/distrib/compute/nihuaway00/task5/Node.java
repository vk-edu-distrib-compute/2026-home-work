package company.vk.edu.distrib.compute.nihuaway00.task5;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Node implements Runnable {
    private static final long POLL_TIMEOUT_MS = 100;
    private static final long PING_INTERVAL_MS = 400;
    private static final long PING_TIMEOUT_MS = 1_200;
    private static final long ELECTION_TIMEOUT_MS = 800;

    private final int id;
    private final NodeRegistry registry;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Object stateLock = new Object();

    private volatile NodeState state = NodeState.FOLLOWER;
    private volatile boolean enabled = true;
    private volatile boolean running = true;
    private volatile int currentLeaderId = -1;
    private volatile long lastPingSentAt = 0;
    private volatile long lastLeaderAnswerAt = System.currentTimeMillis();
    private volatile long electionAnswerDeadlineAt = 0;
    private volatile long victoryDeadlineAt = 0;
    private volatile boolean receivedHigherAnswer = false;

    public Node(NodeRegistry registry, int id) {
        this.registry = registry;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public NodeState getState() {
        return state;
    }

    public int getCurrentLeaderId() {
        return currentLeaderId;
    }

    public boolean isAlive() {
        return enabled;
    }

    public void disable() {
        synchronized (stateLock) {
            enabled = false;
            state = NodeState.DOWN;
            currentLeaderId = -1;
            resetElectionStateLocked();
        }
    }

    public void enable() {
        synchronized (stateLock) {
            if (enabled) {
                return;
            }
            enabled = true;
            state = NodeState.FOLLOWER;
            currentLeaderId = -1;
            lastPingSentAt = 0;
            lastLeaderAnswerAt = System.currentTimeMillis();
            resetElectionStateLocked();
        }
        startElection();
    }

    public void shutdown() {
        running = false;
    }

    void enqueue(Message message) {
        inbox.offer(message);
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = inbox.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (message != null) {
                    processMessage(message);
                }
                if (!enabled) {
                    continue;
                }

                long now = System.currentTimeMillis();
                if (state == NodeState.FOLLOWER) {
                    evaluateFollowerState(now);
                } else if (state == NodeState.CANDIDATE) {
                    evaluateCandidateState(now);
                }
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void processMessage(Message message) {
        if (!enabled) {
            return;
        }

        switch (message.type()) {
            case PING -> handlePing(message);
            case ELECT -> handleElect(message);
            case ANSWER -> handleAnswer(message);
            case VICTORY -> handleVictory(message);
        }
    }

    private void evaluateFollowerState(long now) {
        int leaderToPing = -1;
        boolean needElection = false;
        synchronized (stateLock) {
            if (!enabled || state != NodeState.FOLLOWER) {
                return;
            }
            if (currentLeaderId < 0) {
                needElection = true;
            } else {
                if (now - lastPingSentAt >= PING_INTERVAL_MS) {
                    leaderToPing = currentLeaderId;
                    lastPingSentAt = now;
                }
                if (now - lastLeaderAnswerAt > PING_TIMEOUT_MS) {
                    needElection = true;
                }
            }
        }

        if (leaderToPing >= 0) {
            registry.sendTo(leaderToPing, new Message(MessageType.PING, id));
        }
        if (needElection) {
            startElection();
        }
    }

    private void evaluateCandidateState(long now) {
        boolean shouldBecomeLeader = false;
        boolean shouldRestartElection = false;
        synchronized (stateLock) {
            if (!enabled || state != NodeState.CANDIDATE) {
                return;
            }
            if (!receivedHigherAnswer && now >= electionAnswerDeadlineAt) {
                shouldBecomeLeader = true;
            } else if (receivedHigherAnswer && now >= victoryDeadlineAt) {
                shouldRestartElection = true;
            }
        }

        if (shouldBecomeLeader) {
            becomeLeader();
        } else if (shouldRestartElection) {
            startElection();
        }
    }

    private void handlePing(Message msg) {
        if (state == NodeState.LEADER) {
            registry.sendTo(msg.senderNodeId(), new Message(MessageType.ANSWER, id));
        }
    }

    private void handleElect(Message msg) {
        if (id <= msg.senderNodeId()) {
            return;
        }

        registry.sendTo(msg.senderNodeId(), new Message(MessageType.ANSWER, id));
        if (state != NodeState.LEADER) {
            startElection();
        }
    }

    private void handleAnswer(Message msg) {
        long now = System.currentTimeMillis();
        synchronized (stateLock) {
            if (!enabled) {
                return;
            }
            if (state == NodeState.CANDIDATE) {
                receivedHigherAnswer = true;
                victoryDeadlineAt = now + ELECTION_TIMEOUT_MS;
                return;
            }
            if (state == NodeState.FOLLOWER && msg.senderNodeId() == currentLeaderId) {
                lastLeaderAnswerAt = now;
            }
        }
    }

    private void handleVictory(Message msg) {
        if (msg.senderNodeId() < id) {
            startElection();
            return;
        }
        synchronized (stateLock) {
            if (!enabled) {
                return;
            }
            currentLeaderId = msg.senderNodeId();
            lastLeaderAnswerAt = System.currentTimeMillis();
            lastPingSentAt = 0;
            resetElectionStateLocked();
            state = currentLeaderId == id ? NodeState.LEADER : NodeState.FOLLOWER;
        }
    }

    private void startElection() {
        synchronized (stateLock) {
            if (!enabled) {
                return;
            }
            state = NodeState.CANDIDATE;
            currentLeaderId = -1;
            receivedHigherAnswer = false;
            electionAnswerDeadlineAt = System.currentTimeMillis() + ELECTION_TIMEOUT_MS;
            victoryDeadlineAt = electionAnswerDeadlineAt;
        }

        List<Node> higherNodes = registry.getNodesWithHigherId(id);
        for (Node higherNode : higherNodes) {
            registry.sendTo(higherNode.getId(), new Message(MessageType.ELECT, id));
        }
        if (higherNodes.isEmpty()) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            if (!enabled) {
                return;
            }
            state = NodeState.LEADER;
            currentLeaderId = id;
            lastLeaderAnswerAt = System.currentTimeMillis();
            lastPingSentAt = 0;
            resetElectionStateLocked();
        }
        broadcastVictory();
    }

    private void broadcastVictory() {
        registry.broadcast(new Message(MessageType.VICTORY, id), id);
    }

    private void resetElectionStateLocked() {
        receivedHigherAnswer = false;
        electionAnswerDeadlineAt = 0;
        victoryDeadlineAt = 0;
    }
}
