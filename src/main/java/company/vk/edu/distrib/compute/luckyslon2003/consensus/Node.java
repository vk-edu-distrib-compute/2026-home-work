package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single node in the distributed cluster.
 *
 * <p>Each node runs two daemon threads:
 * <ul>
 *   <li><b>Message-processing loop</b> – drains {@link #inbox} and handles
 *       every incoming {@link Message}.</li>
 *   <li><b>Background scheduler</b> – periodically sends PING to the current
 *       leader and (with a configurable probability) simulates random
 *       failures / recoveries.</li>
 * </ul>
 *
 * <p>Leader election follows the <em>Bully algorithm</em>:
 * <ol>
 *   <li>A node sends ELECT to all nodes with a higher ID.</li>
 *   <li>If no ANSWER is received within {@code ELECTION_TIMEOUT_MS}, the
 *       node declares itself leader and broadcasts VICTORY.</li>
 *   <li>Any node that receives ELECT sends back ANSWER and starts its own
 *       election (if it is not already in one).</li>
 *   <li>A node that receives VICTORY updates its stored leader ID.</li>
 * </ol>
 */
public class Node implements Runnable {

    // -----------------------------------------------------------------------
    // Timing constants (all in milliseconds)
    // -----------------------------------------------------------------------

    /** How long to wait for an ANSWER during an election. */
    static final long ELECTION_TIMEOUT_MS = 1_500;

    /** Interval between PING messages sent to the leader. */
    static final long PING_INTERVAL_MS = 1_000;

    /** How long a follower waits for a PING reply before declaring leader dead. */
    static final long PING_TIMEOUT_MS = 2_000;

    /** Interval at which random failure / recovery is evaluated. */
    static final long CHAOS_INTERVAL_MS = 5_000;

    /** Probability (0.0 – 1.0) that a live node fails in each chaos tick. */
    static final double FAILURE_PROBABILITY = 0.15;

    /** How long a failed node stays down before auto-recovering. */
    static final long RECOVERY_DELAY_MS = 6_000;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    private final int id;

    /**
     * Reference to all nodes in the cluster (set by Cluster after construction).
     */
    private Map<Integer, Node> peers;

    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.FOLLOWER);

    /** ID of the node currently believed to be the leader; -1 = unknown. */
    private final AtomicInteger leaderId = new AtomicInteger(-1);

    /** Set to {@code true} when the node's main loop should stop. */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    // Election bookkeeping (accessed only inside the message-processing thread
    // or under electionLock)
    private final ReentrantLock electionLock = new ReentrantLock();
    private boolean waitingForElectionAnswer;
    private long electionStartedAt;

    // Ping bookkeeping (accessed only inside the message-processing thread
    // or under pingLock)
    private final ReentrantLock pingLock = new ReentrantLock();
    private long lastPingResponseAt = System.currentTimeMillis();

    private final ScheduledExecutorService scheduler;

    // -----------------------------------------------------------------------
    // Construction / lifecycle
    // -----------------------------------------------------------------------

    public Node(int id) {
        this.id = id;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-" + id + "-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Called by {@link Cluster} after all nodes have been created.
     */
    public void setPeers(Map<Integer, Node> peers) {
        this.peers = peers;
    }

    /** Returns this node's unique identifier. */
    public int getId() {
        return id;
    }

    /** Returns the current observable state (safe for external monitoring). */
    public NodeState getState() {
        return state.get();
    }

    /** Returns the ID of the leader as known by this node. */
    public int getLeaderId() {
        return leaderId.get();
    }

    // -----------------------------------------------------------------------
    // External control API
    // -----------------------------------------------------------------------

    /**
     * Forcibly takes this node down (simulates a crash).
     * The node stops responding to messages until {@link #forceRecover()} is called.
     */
    public void forceDown() {
        if (state.getAndSet(NodeState.DOWN) != NodeState.DOWN) {
            log("Forced DOWN");
            inbox.clear(); // drop pending messages
        }
    }

    /**
     * Brings a downed node back online and triggers a new election.
     */
    public void forceRecover() {
        if (state.compareAndSet(NodeState.DOWN, NodeState.FOLLOWER)) {
            log("Forced RECOVER – starting election");
            startElection();
        }
    }

    // -----------------------------------------------------------------------
    // Message delivery (called by the SENDER's thread)
    // -----------------------------------------------------------------------

    /**
     * Delivers a message to this node's inbox.
     * If the node is DOWN, the message is silently dropped.
     */
    public void deliver(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return; // failed node ignores all incoming messages
        }
        inbox.offer(msg);
    }

    // -----------------------------------------------------------------------
    // Main processing loop (runs in its own thread)
    // -----------------------------------------------------------------------

    @Override
    public void run() {
        // Start the background scheduler for pings and chaos
        scheduler.scheduleAtFixedRate(this::pingTask, PING_INTERVAL_MS, PING_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::chaosTask, CHAOS_INTERVAL_MS, CHAOS_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        while (!stopped.get()) {
            try {
                Message msg = inbox.poll(200, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    processMessage(msg);
                }
                // Check election timeout while looping
                checkElectionTimeout();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        scheduler.shutdownNow();
        log("Stopped");
    }

    // -----------------------------------------------------------------------
    // Message handling
    // -----------------------------------------------------------------------

    private void processMessage(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return; // safety: drop messages when down
        }

        switch (msg.getType()) {
            case PING -> handlePing(msg);
            case ANSWER -> handleAnswer(msg);
            case ELECT -> handleElect(msg);
            case VICTORY -> handleVictory(msg);
        }
    }

    private void handlePing(Message msg) {
        // Only the leader answers PINGs
        if (state.get() == NodeState.LEADER) {
            send(msg.getSenderId(), Message.Type.ANSWER);
        }
    }

    private void handleAnswer(Message msg) {
        // Reset ping watchdog whenever we hear from the leader
        pingLock.lock();
        try {
            lastPingResponseAt = System.currentTimeMillis();
        } finally {
            pingLock.unlock();
        }

        // If we're in election and received an ANSWER, a higher node is alive –
        // stop waiting (the higher node will finish the election)
        electionLock.lock();
        try {
            if (waitingForElectionAnswer) {
                log("Received ANSWER from node " + msg.getSenderId() + " – backing off election");
                waitingForElectionAnswer = false;
                state.compareAndSet(NodeState.ELECTION, NodeState.FOLLOWER);
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void handleElect(Message msg) {
        // We have a higher ID, so we respond and start our own election
        send(msg.getSenderId(), Message.Type.ANSWER);
        log("Received ELECT from node " + msg.getSenderId()
                + " – responding ANSWER and starting own election");
        startElection();
    }

    private void handleVictory(Message msg) {
        int newLeader = msg.getSenderId();
        leaderId.set(newLeader);
        electionLock.lock();
        try {
            waitingForElectionAnswer = false;
        } finally {
            electionLock.unlock();
        }
        pingLock.lock();
        try {
            lastPingResponseAt = System.currentTimeMillis();
        } finally {
            pingLock.unlock();
        }
        if (newLeader == id) {
            state.set(NodeState.LEADER);
            log("I am the new LEADER");
        } else {
            state.compareAndSet(NodeState.ELECTION, NodeState.FOLLOWER);
            state.compareAndSet(NodeState.LEADER, NodeState.FOLLOWER);
            log("Acknowledged new LEADER: node " + newLeader);
        }
    }

    // -----------------------------------------------------------------------
    // Election logic
    // -----------------------------------------------------------------------

    /**
     * Initiates the Bully election algorithm.
     * Sends ELECT to all nodes with higher IDs and waits for ANSWER responses.
     * If none reply within {@value #ELECTION_TIMEOUT_MS} ms, this node wins.
     */
    public void startElection() {
        if (state.get() == NodeState.DOWN) {
            return;
        }

        electionLock.lock();
        try {
            if (waitingForElectionAnswer) {
                // Already in an election – refresh the timer
                electionStartedAt = System.currentTimeMillis();
                return;
            }

            state.set(NodeState.ELECTION);
            log("Starting election");

            // Find nodes with higher IDs
            List<Integer> higherNodes = peers.keySet().stream()
                    .filter(nid -> nid > id)
                    .toList();

            if (higherNodes.isEmpty()) {
                // We are the highest ID – declare victory immediately
                declareVictory();
                return;
            }

            // Send ELECT to all higher-ID nodes
            for (int nid : higherNodes) {
                send(nid, Message.Type.ELECT);
            }

            waitingForElectionAnswer = true;
            electionStartedAt = System.currentTimeMillis();
        } finally {
            electionLock.unlock();
        }
    }

    /**
     * Checked periodically inside the message loop.
     * If the election timeout has expired and no ANSWER was received,
     * this node declares itself the winner.
     */
    private void checkElectionTimeout() {
        electionLock.lock();
        try {
            if (waitingForElectionAnswer
                    && System.currentTimeMillis() - electionStartedAt > ELECTION_TIMEOUT_MS) {
                log("Election timeout – no higher node responded, declaring VICTORY");
                declareVictory();
            }
        } finally {
            electionLock.unlock();
        }
    }

    /** Broadcast VICTORY to all peers and update own state. */
    private void declareVictory() {
        electionLock.lock();
        try {
            waitingForElectionAnswer = false;
        } finally {
            electionLock.unlock();
        }
        leaderId.set(id);
        state.set(NodeState.LEADER);
        log("Broadcasting VICTORY");
        for (int nid : peers.keySet()) {
            if (nid != id) {
                send(nid, Message.Type.VICTORY);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Background tasks
    // -----------------------------------------------------------------------

    /** Periodically ping the leader to check it is still alive. */
    private void pingTask() {
        if (state.get() == NodeState.DOWN || state.get() == NodeState.LEADER) {
            return; // leaders don't ping themselves; downed nodes do nothing
        }

        int currentLeader = leaderId.get();
        if (currentLeader < 0) {
            // No known leader – start an election
            startElection();
            return;
        }

        send(currentLeader, Message.Type.PING);

        // Check if the last PING response is within the timeout window
        pingLock.lock();
        try {
            long silence = System.currentTimeMillis() - lastPingResponseAt;
            if (silence > PING_TIMEOUT_MS) {
                log("Leader node " + currentLeader + " is unresponsive (silence "
                        + silence + " ms) – starting election");
                leaderId.set(-1);
                startElection();
            }
        } finally {
            pingLock.unlock();
        }
    }

    /** Randomly fail or recover the node to simulate real-world chaos. */
    private void chaosTask() {
        NodeState current = state.get();
        if (current == NodeState.DOWN) {
            // Node is already down – schedule an auto-recovery
            // (recovery is handled by forceRecover, triggered from the scheduler
            // after RECOVERY_DELAY_MS)
            return;
        }

        if (Math.random() < FAILURE_PROBABILITY) {
            log("CHAOS: random failure triggered");
            forceDown();
            // Schedule recovery after a delay
            scheduler.schedule(this::forceRecover, RECOVERY_DELAY_MS, TimeUnit.MILLISECONDS);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Send a message from this node to the target node. */
    private void send(int targetId, Message.Type type) {
        Node target = peers.get(targetId);
        if (target != null) {
            target.deliver(new Message(type, id));
        }
    }

    /** Structured log with timestamp and state. */
    void log(String msg) {
        java.util.logging.Logger.getLogger(Node.class.getName())
                .info(String.format("[%6d ms] Node %2d [%-8s] leader=%-2s | %s%n",
                        System.currentTimeMillis() % 1_000_000,
                        id,
                        state.get(),
                        leaderId.get() < 0 ? "?" : leaderId.get(),
                        msg));
    }

    /** Gracefully stop the node's processing loop. */
    public void stop() {
        stopped.set(true);
        scheduler.shutdownNow();
    }
}