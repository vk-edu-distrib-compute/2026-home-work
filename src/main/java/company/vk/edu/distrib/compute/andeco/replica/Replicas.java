package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.andeco.FileDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Replicas {
    private static final Logger log = LoggerFactory.getLogger(Replicas.class);

    private final int numberOfReplicas;
    private final List<Replica> replicaList;
    private final Map<Integer, Replica> replicaMap;
    private final AtomicReference<BigInteger> version = new AtomicReference<>(BigInteger.ZERO);
    private final ExecutorService executor;

    public Replicas(int port, int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        this.replicaList = new ArrayList<>(numberOfReplicas);
        this.replicaMap = new HashMap<>(numberOfReplicas);
        this.executor = Executors.newFixedThreadPool(Math.max(1, numberOfReplicas));

        for (int i = 0; i < numberOfReplicas; i++) {
            Replica replica = createReplica(port, i);
            replicaList.add(replica);
            replicaMap.put(i, replica);
        }
    }

    private Replica createReplica(int port, int replicaId) {
        try {
            return new Replica(replicaId, new FileDao(port + "-" + replicaId));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Replica getReplica(int id) {
        return replicaMap.get(id);
    }

    public void disableNode(int id) {
        Replica replica = replicaMap.get(id);
        if (replica != null) {
            replica.setAvailable(false);
        }
    }

    public void enableNode(int id) {
        Replica replica = replicaMap.get(id);
        if (replica != null) {
            replica.setAvailable(true);
        }
    }

    private BigInteger nextVersion() {
        return version.updateAndGet(v -> v.add(BigInteger.ONE));
    }

    public void writeData(int ack, String key, byte[] value) {
        BigInteger ver = nextVersion();
        AtomicInteger success = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>(replicaList.size());

        for (Replica replica : replicaList) {
            if (!replica.isAvailable()) {
                continue;
            }

            futures.add(executor.submit(() -> {
                try {
                    replica.write(key, value, ver);
                    success.incrementAndGet();
                } catch (Exception e) {
                    log.warn("replica write failed", e);
                }
            }));
        }

        waitAll(futures);

        if (success.get() < ack) {
            throw new IllegalStateException("Write quorum not reached");
        }
    }

    public void deleteData(int ack, String key) {
        BigInteger ver = nextVersion();
        AtomicInteger success = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>(replicaList.size());

        for (Replica replica : replicaList) {
            if (!replica.isAvailable()) {
                continue;
            }

            futures.add(executor.submit(() -> {
                try {
                    replica.delete(key, ver);
                    success.incrementAndGet();
                } catch (Exception e) {
                    log.warn("replica delete failed", e);
                }
            }));
        }

        waitAll(futures);

        if (success.get() < ack) {
            throw new IllegalStateException("Delete quorum not reached");
        }
    }

    public ReplicaValue readData(int ack, String key) {
        AtomicInteger success = new AtomicInteger();
        ConcurrentLinkedQueue<ReplicaValue> replies = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>(replicaList.size());

        for (Replica replica : replicaList) {
            if (!replica.isAvailable()) {
                continue;
            }

            futures.add(executor.submit(() -> {
                try {
                    ReplicaValue value = replica.read(key);
                    if (value != null) {
                        replies.add(value);
                    }
                    success.incrementAndGet();
                } catch (Exception e) {
                    log.warn("replica read failed", e);
                }
            }));
        }

        waitAll(futures);

        if (success.get() < ack) {
            throw new IllegalStateException("Not enough replicas responded");
        }

        return replies.stream()
                .max(Comparator.comparing(ReplicaValue::version))
                .orElse(null);
    }

    private void waitAll(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                log.warn("task execution failed", e);
            }
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
