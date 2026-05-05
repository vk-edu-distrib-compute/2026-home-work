package company.vk.edu.distrib.compute.arseniy90.consensus;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionManager.class.getName());

    private final int id;
    private final Map<Integer, Node> cluster;
    private final BlockingQueue<Message> messageQueue;

    private final AtomicBoolean isRunning;
    private final NodeState state;
    private final ElectionManager electionManager;
    private final MessageProcessor messageProcessor;

    private final ScheduledExecutorService electionScheduler;
    private Thread nodeThread;
    private Thread pingThread;

    public Node(int id, Map<Integer, Node> cluster) {
        this.id = id;
        this.cluster = cluster;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.isRunning = new AtomicBoolean(false);

        this.state = new NodeState(id);
        this.electionScheduler = Executors.newSingleThreadScheduledExecutor();
        this.electionManager = new ElectionManager(id, cluster, state, electionScheduler);
        this.messageProcessor = new MessageProcessor(id, cluster, state, electionManager);
    }

    public int getId() {
        return id;
    }

    public int getLeaderId() {
        return state.getLeaderId();
    }

    public NodeRole getRole() {
        return state.getRole();
    }

    public void setRunning(boolean running) {
        if (!running) {
            LOGGER.warn("Node {}: Crash", id);
            state.setLeaderId(-1);
            state.compareAndSetElection(true, false);
            messageQueue.clear();
            state.setRole(NodeRole.DOWN);
            isRunning.set(false);
            return;
        } 

        isRunning.set(true);
        LOGGER.warn("Node {}: Recovered", id);
        state.resetLeaderContactTime();
        state.setRole(NodeRole.FOLLOWER);
        electionManager.startElection();
    }

    public void gracefulShutdown() {
        LOGGER.info("Node {}: Start graceful shutdown", id);
        if (state.getLeaderId() == id) {
            cluster.forEach((k, node) -> {
                if (k != id) {
                    node.receiveMessage(new Message(MessageType.RESIGN, id, "I am leaving"));
                }
            });
        }
        state.setLeaderId(-1);
        state.compareAndSetElection(true, false);
        messageQueue.clear();
        state.setRole(NodeRole.DOWN);
        isRunning.set(false);
        LOGGER.info("Node {}: Finished graceful shutdown", id);
    }

    public void receiveMessage(Message msg) {
        if (!isRunning.get()) {
            return;
        }
        try {
            messageQueue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        isRunning.set(true);
        state.setRole(NodeRole.FOLLOWER);
        state.resetLeaderContactTime();

        runPing();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message msg = messageQueue.poll(500, TimeUnit.MILLISECONDS);
                if (msg != null && isRunning.get()) {
                    messageProcessor.process(msg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void runPing() {
        pingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (isRunning.get()) {
                    processPing();
                }
            }
        });
        pingThread.setDaemon(true);
        pingThread.start();
    }

    private void processPing() {    
        int currentLeader = state.getLeaderId();
        if (currentLeader != -1 && currentLeader != id) {
            Node leaderNode = cluster.get(currentLeader);
            if (leaderNode != null) {
                leaderNode.receiveMessage(new Message(MessageType.PING, id, "ping msg"));
            }

            if (System.currentTimeMillis() - state.getLastLeaderContactTime() > 3000) {
                LOGGER.info("Node {}: Leader {} timeout. Start elections.", id, currentLeader);
                state.setLeaderId(-1);
                state.setRole(NodeRole.FOLLOWER);
                electionManager.startElection();
            }
        }
    }

    public void shutdown() {
        if (pingThread != null) {
            pingThread.interrupt();
        }
        if (nodeThread != null) {
            nodeThread.interrupt();
        }
        electionScheduler.shutdown();
    }

    public void start() {
        nodeThread = new Thread(this, "Node-" + id);
        nodeThread.start();
        electionManager.startElection();
    }
}
