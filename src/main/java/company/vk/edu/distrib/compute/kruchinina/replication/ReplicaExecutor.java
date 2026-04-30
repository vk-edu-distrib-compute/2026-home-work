package company.vk.edu.distrib.compute.kruchinina.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Управляет репликами FileSystemDao, выполняет параллельные операции
 * и собирает статистику обращений.
 */
public class ReplicaExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaExecutor.class);
    private static final int MIN_REPLICA_COUNT = 1;

    private final int replicaCount;
    private final List<FileSystemDao> replicas;
    private final ExecutorService executor;
    private final boolean[] enabled;
    private final AtomicInteger[] readCounters;
    private final AtomicInteger[] writeCounters;

    public ReplicaExecutor(String baseDir, int n) throws IOException {
        if (n < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException(
                    "Number of replicas must be at least " + MIN_REPLICA_COUNT);
        }
        this.replicaCount = n;
        this.replicas = new ArrayList<>(n);
        this.enabled = new boolean[n];
        this.readCounters = new AtomicInteger[n];
        this.writeCounters = new AtomicInteger[n];
        this.executor = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            Path replicaDir = Path.of(baseDir + "_r" + i);
            Files.createDirectories(replicaDir);
            replicas.add(new FileSystemDao(replicaDir.toString()));
            enabled[i] = true;
            readCounters[i] = new AtomicInteger(0);
            writeCounters[i] = new AtomicInteger(0);
        }
    }

    /**
     * Подготавливает асинхронные задачи чтения с активных реплик.
     */
    public List<CompletableFuture<byte[]>> submitReadTasks(String key) {
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (enabled[idx]) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        byte[] data = replicas.get(idx).get(key);
                        readCounters[idx].incrementAndGet();
                        return data;
                    } catch (IOException e) {
                        return null;
                    }
                }, executor));
            }
        }
        return futures;
    }

    /**
     * Параллельная запись с контролем кворума.
     */
    public void upsert(String key, byte[] value, int ack) throws IOException {
        validateAck(ack);
        CountDownLatch latch = new CountDownLatch(replicaCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (!enabled[idx]) {
                latch.countDown();
                continue;
            }
            executor.submit(() -> {
                try {
                    replicas.get(idx).upsert(key, value);
                    successCount.incrementAndGet();
                    writeCounters[idx].incrementAndGet();
                } catch (IOException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Replica {} failed to upsert key {}: {}", idx, key, e.getMessage());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        awaitLatch(latch);
        if (successCount.get() < ack) {
            throw new IOException("Upsert failed: only " + successCount.get()
                    + " replicas acknowledged, need " + ack);
        }
    }

    /**
     * Параллельное удаление с контролем кворума.
     */
    public void delete(String key, int ack) throws IOException {
        validateAck(ack);
        CountDownLatch latch = new CountDownLatch(replicaCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (!enabled[idx]) {
                latch.countDown();
                continue;
            }
            executor.submit(() -> {
                try {
                    replicas.get(idx).delete(key);
                    successCount.incrementAndGet();
                } catch (IOException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Replica {} failed to delete key {}: {}", idx, key, e.getMessage());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        awaitLatch(latch);
        if (successCount.get() < ack) {
            throw new IOException("Delete failed: only " + successCount.get()
                    + " replicas acknowledged, need " + ack);
        }
    }

    public void disable(int nodeId) {
        checkNodeId(nodeId);
        enabled[nodeId] = false;
    }

    public void enable(int nodeId) {
        checkNodeId(nodeId);
        enabled[nodeId] = true;
    }

    public int getKeyCount(int replicaIndex) {
        checkNodeId(replicaIndex);
        return replicas.get(replicaIndex).countKeys();
    }

    public int getReadAccessCount(int replicaIndex) {
        checkNodeId(replicaIndex);
        return readCounters[replicaIndex].get();
    }

    public int getWriteAccessCount(int replicaIndex) {
        checkNodeId(replicaIndex);
        return writeCounters[replicaIndex].get();
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    public void shutdownAndClose() throws IOException {
        executor.shutdownNow();
        for (FileSystemDao dao : replicas) {
            dao.close();
        }
    }

    private void validateAck(int ack) {
        if (ack > replicaCount || ack < 1) {
            throw new IllegalArgumentException("Invalid ack: " + ack
                    + " (must be 1.." + replicaCount + ")");
        }
    }

    private void checkNodeId(int nodeId) {
        if (nodeId < 0 || nodeId >= replicaCount) {
            throw new IllegalArgumentException("Invalid replica index: " + nodeId);
        }
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS) && LOG.isWarnEnabled()) {
                LOG.warn("Timeout waiting for replica operations");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
