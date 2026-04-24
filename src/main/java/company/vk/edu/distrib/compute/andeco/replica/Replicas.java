package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.andeco.FileDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Replicas {
    private static final Logger log = LoggerFactory.getLogger(Replicas.class);
    private final int numberOfReplicas;
    private final List<Replica> replicas;
    private final AtomicReference<BigInteger> version = new AtomicReference<>(BigInteger.ZERO);

    public Replicas(int port, int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        this.replicas = new ArrayList<>();

        for (int i = 0; i < numberOfReplicas; i++) {
            try {
                replicas.add(new Replica(i, new FileDao(port + "-" + i)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Replica getReplica(int id) {
        return replicas.stream()
                .filter(r -> r.getId() == id)
                .findFirst()
                .orElse(null);
    }

    public void disableNode(int id) {
        Replica replica = getReplica(id);
        if (replica != null) {
            replica.setAvailable(false);
        }
    }

    public void enableNode(int id) {
        Replica replica = getReplica(id);
        if (replica != null) {
            replica.setAvailable(true);
        }
    }

    private BigInteger nextVersion() {
        return version.updateAndGet(v -> v.add(BigInteger.ONE));
    }

    public int writeData(int ack, String key, byte[] value) {
        BigInteger ver = nextVersion();
        AtomicInteger success = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (Replica replica : replicas) {
            if (!replica.isAvailable()) {
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    replica.write(key, value, ver);
                    success.incrementAndGet();
                } catch (Exception ignored) {
                    log.warn("replica is ignored");
                }
            });
            threads.add(t);
            t.start();
        }

        joinAll(threads);
        return success.get();
    }

    public int deleteData(int ack, String key) {
        BigInteger ver = nextVersion();
        AtomicInteger success = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (Replica replica : replicas) {
            if (!replica.isAvailable()) {
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    replica.delete(key, ver);
                    success.incrementAndGet();
                } catch (Exception ignored) {
                    log.warn("replica is ignored");
                }
            });
            threads.add(t);
            t.start();
        }

        joinAll(threads);
        return success.get();
    }

    public ReplicaValue readData(int ack, String key) {
        List<ReplicaValue> replies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger success = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (Replica replica : replicas) {
            if (!replica.isAvailable()) {
                continue;
            }
            Thread t = new Thread(() -> {
                try {
                    replies.add(replica.read(key));
                    success.incrementAndGet();
                } catch (Exception ignored) {
                    log.warn("replica is ignored");
                }
            });
            threads.add(t);
            t.start();
        }

        joinAll(threads);

        if (success.get() < ack) {
            throw new IllegalStateException();
        }

        return replies.stream()
                .filter(Objects::nonNull)
                .max(Comparator.comparing(ReplicaValue::version))
                .orElse(null);
    }

    private static void joinAll(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
