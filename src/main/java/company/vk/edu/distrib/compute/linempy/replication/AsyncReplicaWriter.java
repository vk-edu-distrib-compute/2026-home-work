package company.vk.edu.distrib.compute.linempy.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/**
 * Асинхронная/синхронная запись реплик.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class AsyncReplicaWriter {
    private static final Logger log = LoggerFactory.getLogger(AsyncReplicaWriter.class);

    private final ReplicaStorage storage;
    private final ReplicaStats stats;
    private final ExecutorService executor;
    private final boolean asyncMode;

    public AsyncReplicaWriter(ReplicaStorage storage, ReplicaStats stats, int poolSize, boolean asyncMode) {
        this.storage = storage;
        this.stats = stats;
        this.asyncMode = asyncMode;
        this.executor = Executors.newFixedThreadPool(poolSize);
    }

    public int write(String key, byte[] value, int requiredAck, List<Integer> indexes, IntPredicate isEnabled) {
        if (asyncMode) {
            return writeAsync(key, value, requiredAck, indexes, isEnabled);
        } else {
            return writeSync(key, value, indexes, isEnabled);
        }
    }

    private int writeSync(String key, byte[] value, List<Integer> indexes, IntPredicate isEnabled) {
        int success = 0;
        for (int idx : indexes) {
            if (!isEnabled.test(idx)) {
                continue;
            }
            try {
                storage.write(key, idx, value);
                success++;
                stats.recordWrite(idx);
            } catch (IOException e) {
                log.error("Write failed idx={}", idx);
            }
        }
        return success;
    }

    private int writeAsync(String key, byte[] value, int requiredAck, List<Integer> indexes, IntPredicate isEnabled) {
        AtomicInteger success = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int idx : indexes) {
            if (!isEnabled.test(idx)) {
                continue;
            }
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    storage.write(key, idx, value);
                    success.incrementAndGet();
                    stats.recordWrite(idx);
                } catch (IOException e) {
                    log.warn("Write failed idx={}", idx);
                }
            }, executor));
        }

        CompletableFuture<Void> ackFuture = CompletableFuture.runAsync(() -> {
            while (success.get() < requiredAck) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        try {
            ackFuture.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Timeout waiting for ack={}", requiredAck);
        }
        return success.get();
    }

    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
