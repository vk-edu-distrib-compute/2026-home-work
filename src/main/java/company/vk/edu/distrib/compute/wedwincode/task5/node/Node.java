package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Node implements Runnable {
    private static final long MESSAGE_POLL_TIMEOUT_MS = 200;

    private final int id;
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    private final ElectionManager electionManager;
    private final LeaderMonitor leaderMonitor;
    private final FailureManager failureManager;
    private final GracefulShutdownManager gracefulShutdownManager;

    private Map<Integer, Node> nodes = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean manuallyEnabled = new AtomicBoolean(true);
    private final AtomicBoolean crashed = new AtomicBoolean(false);

    private final AtomicReference<State> state = new AtomicReference<>(State.FOLLOWER);
    private final AtomicInteger leaderId = new AtomicInteger(-1);

    public Node(int id) {
        this.id = id;
        this.electionManager = new ElectionManager(this, messages);
        this.leaderMonitor = new LeaderMonitor(this);
        this.failureManager = new FailureManager(this);
        this.gracefulShutdownManager = new GracefulShutdownManager(this);
    }

    public int getId() {
        return id;
    }

    public State getState() {
        return state.get();
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public boolean isAlive() {
        return manuallyEnabled.get() && !crashed.get();
    }

    public void setCluster(Map<Integer, Node> nodes) {
        this.nodes = nodes;
    }

    public void setEnabled(boolean value) {
        manuallyEnabled.set(value);

        if (value) {
            enableNode();
        } else {
            disableNode();
        }
    }

    public void setRandomFailuresEnabled(boolean value) {
        failureManager.setRandomFailuresEnabled(value);
    }

    public void stop() {
        running.set(false);
    }

    public boolean receiveMessage(Message message) {
        if (!isAlive()) {
            return false;
        }

        messages.offer(message);
        return true;
    }

    public void gracefulShutdown() {
        gracefulShutdownManager.gracefulShutdown();
    }

    void handleMessage(Message message) {
        if (!isAlive()) {
            return;
        }

        switch (message.type()) {
            case PING -> handlePing(message);
            case ELECT -> handleElect(message);
            case ANSWER -> handleAnswer(message);
            case VICTORY -> handleVictory(message);
        }
    }

    void handleVictory(Message message) {
        int newLeaderId = message.senderId();

        if (newLeaderId > id) {
            acceptLeader(newLeaderId);
            return;
        }

        if (newLeaderId < id && isAlive()) {
            electionManager.startElection();
            return;
        }

        if (newLeaderId == id) {
            becomeLeaderLocally();
        }
    }

    Map<Integer, Node> getCluster() {
        return nodes;
    }

    ElectionManager getElectionManager() {
        return electionManager;
    }

    boolean sendMessageTo(Node node, Message message) {
        return sendMessage(node, message);
    }

    boolean isManuallyEnabled() {
        return manuallyEnabled.get();
    }

    boolean isCrashed() {
        return crashed.get();
    }

    void clearLeaderAndBecomeFollower() {
        state.set(State.FOLLOWER);
        leaderId.set(-1);
    }

    void setLeaderState(int newLeaderId) {
        leaderId.set(newLeaderId);
        state.set(State.LEADER);
        leaderMonitor.resetTimers();
    }

    void setFollowerWithLeader(int newLeaderId) {
        leaderId.set(newLeaderId);
        state.set(State.FOLLOWER);
        leaderMonitor.resetTimers();
    }

    boolean forceBecomeLeaderAfterTransfer() {
        return electionManager.forceBecomeLeaderAfterTransfer();
    }

    void markCrashed() {
        crashed.set(true);
        state.set(State.FOLLOWER);
        leaderId.set(-1);
        electionManager.reset();
        messages.clear();
    }

    void markRecovered() {
        crashed.set(false);
        leaderId.set(-1);
        state.set(State.FOLLOWER);
        electionManager.reset();
        leaderMonitor.resetTimers();
    }

    private void enableNode() {
        ClusterLogger.event(id, "was manually enabled");

        crashed.set(false);
        leaderId.set(-1);
        state.set(State.FOLLOWER);
        electionManager.reset();
        leaderMonitor.resetTimers();

        receiveMessage(new Message(Message.Type.ELECT, id));
    }

    private void disableNode() {
        ClusterLogger.event(id, "was manually disabled");

        state.set(State.FOLLOWER);
        leaderId.set(-1);
        electionManager.reset();
        messages.clear();
    }

    private boolean sendMessage(Node node, Message message) {
        return isAlive() && node.receiveMessage(message);
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

        if (state.get() == State.LEADER) {
            return;
        }

        if (hasAliveKnownLeader()) {
            return;
        }

        if (senderId < id && !electionManager.isInProgress()) {
            electionManager.startElection();
        }
    }

    private boolean hasAliveKnownLeader() {
        int currentLeaderId = leaderId.get();

        if (currentLeaderId == -1) {
            return false;
        }

        Node currentLeader = nodes.get(currentLeaderId);

        return currentLeader != null && currentLeader.isAlive();
    }

    private void handleAnswer(Message message) {
        if (message.senderId() == leaderId.get()) {
            leaderMonitor.markLeaderAnswered();
        }
    }

    private void acceptLeader(int newLeaderId) {
        setFollowerWithLeader(newLeaderId);
        electionManager.reset();

        ClusterLogger.event(id, "accepted leader " + leaderId.get());
    }

    private void becomeLeaderLocally() {
        setLeaderState(id);
        electionManager.reset();
    }

    private void processNextMessage() throws InterruptedException {
        Message message = messages.poll(MESSAGE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (message != null) {
            handleMessage(message);
        }
    }

    private void sleepWhileDown() throws InterruptedException {
        Thread.sleep(MESSAGE_POLL_TIMEOUT_MS);
    }

    @Override
    public void run() {
        receiveMessage(new Message(Message.Type.ELECT, id));

        while (running.get()) {
            try {
                failureManager.maybeRandomCrash();

                if (!isAlive()) {
                    sleepWhileDown();
                    continue;
                }

                processNextMessage();
                leaderMonitor.checkLeader();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
