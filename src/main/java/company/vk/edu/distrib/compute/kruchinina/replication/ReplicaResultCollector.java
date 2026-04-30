package company.vk.edu.distrib.compute.kruchinina.replication;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public final class ReplicaResultCollector {

    private ReplicaResultCollector() {
    }

    private enum FutureResult { SUCCESS, NOT_FOUND, TIMEOUT_OR_ERROR }

    public static byte[] collectReadResults(List<CompletableFuture<byte[]>> futures, int ack)
            throws IOException {
        ResultAccumulator acc = new ResultAccumulator();
        AtomicReference<byte[]> dataRef = new AtomicReference<>(null);

        for (CompletableFuture<byte[]> future : futures) {
            dataRef.set(null);
            processFutureResult(future, dataRef, acc);
            if (acc.successCount >= ack) {
                break;
            }
        }

        if (acc.successCount >= ack) {
            return acc.firstData;
        }
        if (acc.successCount == 0 && acc.notFoundCount == futures.size()) {
            throw new NoSuchElementException("No data for key");
        }
        throw new IOException("Not enough replicas available (need " + ack + ")");
    }

    private static void processFutureResult(CompletableFuture<byte[]> future,
                                            AtomicReference<byte[]> dataRef,
                                            ResultAccumulator acc) {
        FutureResult result = evaluateFuture(future, dataRef);
        if (result == FutureResult.SUCCESS) {
            acc.successCount++;
            if (acc.firstData == null) {
                acc.firstData = dataRef.get();
            }
        } else if (result == FutureResult.NOT_FOUND) {
            acc.notFoundCount++;
        }
    }

    private static FutureResult evaluateFuture(CompletableFuture<byte[]> future,
                                               AtomicReference<byte[]> dataHolder) {
        try {
            byte[] data = future.get(500, TimeUnit.MILLISECONDS);
            dataHolder.set(data);
            return FutureResult.SUCCESS;
        } catch (ExecutionException e) {
            return (e.getCause() instanceof NoSuchElementException)
                    ? FutureResult.NOT_FOUND
                    : FutureResult.TIMEOUT_OR_ERROR;
        } catch (TimeoutException e) {
            return FutureResult.TIMEOUT_OR_ERROR;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureResult.TIMEOUT_OR_ERROR;
        }
    }

    /**
     * Аккумулирует результаты обработки отдельных future-задач.
     * Конструктор по умолчанию инициализирует все поля значениями (0, null).
     */
    private static final class ResultAccumulator {
        int successCount;
        int notFoundCount;
        byte[] firstData;
    }
}
