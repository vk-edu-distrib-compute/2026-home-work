package company.vk.edu.distrib.compute.kruchinina.replication;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Собирает и проверяет результаты асинхронного чтения с реплик.
 */
public final class ReplicaResultCollector {

    private ReplicaResultCollector() {
    }

    private enum FutureResult { SUCCESS, NOT_FOUND, TIMEOUT_OR_ERROR }

    /**
     * Анализирует результаты выполнения CompletableFuture и возвращает количество успешных,
     * а также первое полученное значение. Бросает исключение при невозможности набрать кворум.
     */
    public static byte[] collectReadResults(List<CompletableFuture<byte[]>> futures, int ack)
            throws IOException {
        int success = 0;
        byte[] firstData = null;
        int notFoundCount = 0;
        AtomicReference<byte[]> dataRef = new AtomicReference<>(null);

        for (CompletableFuture<byte[]> future : futures) {
            dataRef.set(null);
            FutureResult result = evaluateFuture(future, dataRef);
            if (result == FutureResult.SUCCESS) {
                success++;
                if (firstData == null) {
                    firstData = dataRef.get();
                }
            } else if (result == FutureResult.NOT_FOUND) {
                notFoundCount++;
            }
            if (success >= ack) {
                break;
            }
        }

        if (success >= ack) {
            return firstData;
        }
        if (success == 0 && notFoundCount == futures.size()) {
            throw new NoSuchElementException("No data for key");
        }
        throw new IOException("Not enough replicas available (need " + ack + ")");
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
}
