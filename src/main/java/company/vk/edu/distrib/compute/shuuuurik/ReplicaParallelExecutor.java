package company.vk.edu.distrib.compute.shuuuurik;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Движок параллельного I/O для операций с репликами.
 *
 * <p>Запускает N задач одновременно через {@link ExecutorCompletionService} и собирает
 * первые ack успешных результатов. Оставшиеся задачи продолжают в фоне - их результаты
 * не используются, но они завершатся самостоятельно.
 *
 * <p>Использует виртуальные потоки ({@link Executors#newVirtualThreadPerTaskExecutor()})
 * для минимальных накладных расходов при I/O-bound операциях.
 */
public class ReplicaParallelExecutor {

    private static final Logger log = LoggerFactory.getLogger(ReplicaParallelExecutor.class);

    private final ExecutorService replicaExecutor;

    ReplicaParallelExecutor() {
        this.replicaExecutor = Executors.newVirtualThreadPerTaskExecutor(); // Виртуальные потоки
    }

    /**
     * Останавливает пул потоков. Вызывается при остановке сервиса.
     */
    void shutdown() {
        replicaExecutor.shutdown();
    }

    /**
     * Запускает все задачи параллельно и собирает первые ack успешных результатов.
     *
     * <p>Алгоритм:
     * <ol>
     *   <li>Все задачи отправляются в {@link ExecutorCompletionService} одновременно</li>
     *   <li>Результаты забираются по мере завершения через {@code take()} (poll из очереди готовых)</li>
     *   <li>При достижении ack успехов - возвращаем список немедленно; остальные задачи продолжают
     *       в фоне и завершатся самостоятельно, их результаты не используются (они уже переданы в executor)</li>
     *   <li>Если математически невозможно набрать ack - выходим досрочно</li>
     * </ol>
     *
     * @param tasks     список задач, каждая возвращает {@link Optional} (пустой = успех без данных)
     * @param ack       требуемое число успешных завершений
     * @param key       ключ для логирования
     * @param operation название операции для логирования
     * @return список {@link VersionedEntry} из успешных read-задач,
     *     пустой список для write-задач, или {@code null} если ack не набран
     */
    List<VersionedEntry> collectSuccesses(
            List<Callable<Optional<VersionedEntry>>> tasks,
            int ack,
            String key,
            String operation
    ) {
        ExecutorCompletionService<Optional<VersionedEntry>> completionService =
                new ExecutorCompletionService<>(replicaExecutor);

        for (Callable<Optional<VersionedEntry>> task : tasks) {
            completionService.submit(task);
        }

        return drainUntilAck(completionService, tasks.size(), ack, key, operation);
    }

    /**
     * Забирает результаты из {@link ExecutorCompletionService} до набора ack успехов.
     *
     * @param completionService сервис с запущенными задачами
     * @param submitted         количество отправленных задач
     * @param ack               порог успехов
     * @param key               ключ для логирования
     * @param operation         операция для логирования
     * @return список найденных значений или {@code null} при нехватке подтверждений
     */
    private List<VersionedEntry> drainUntilAck(
            ExecutorCompletionService<Optional<VersionedEntry>> completionService,
            int submitted,
            int ack,
            String key,
            String operation
    ) {
        List<VersionedEntry> foundValues = new ArrayList<>();
        int successCount = 0;
        int failureCount = 0;

        // Забираем результаты по мере готовности
        for (int i = 0; i < submitted; i++) {
            if (successCount >= ack) {
                break;
            }
            int remaining = submitted - i;
            if (successCount + remaining < ack) {
                // Оставшихся задач не хватит даже в лучшем случае - выходим досрочно
                break;
            }

            ReplicaTaskResult taskResult = awaitNextResult(completionService, key, operation);
            if (taskResult.succeeded()) {
                successCount++;
                taskResult.value().ifPresent(foundValues::add);
            } else {
                failureCount++;
            }
        }

        if (successCount < ack) {
            log.warn("{} key={}: only {}/{} acks (failures={})",
                    operation, key, successCount, ack, failureCount);
            return null;
        }

        return foundValues;
    }

    /**
     * Ожидает следующую завершившуюся фьючу и возвращает результат как {@link ReplicaTaskResult}.
     *
     * @param completionService сервис с запущенными задачами
     * @param key               ключ для логирования
     * @param operation         операция для логирования
     * @return результат задачи: успех с Optional-значением или провал
     */
    private ReplicaTaskResult awaitNextResult(
            ExecutorCompletionService<Optional<VersionedEntry>> completionService,
            String key,
            String operation
    ) {
        try {
            Future<Optional<VersionedEntry>> future = completionService.take();
            return ReplicaTaskResult.success(future.get());
        } catch (ExecutionException e) {
            logReplicaFailure(e.getCause(), key, operation);
            return ReplicaTaskResult.failure();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("{} key={}: interrupted while waiting for replica", operation, key);
            return ReplicaTaskResult.failure();
        }
    }

    /**
     * Логирует ошибку задачи-реплики с подходящим уровнем.
     *
     * @param cause     причина ошибки
     * @param key       ключ для контекста
     * @param operation операция для контекста
     */
    private void logReplicaFailure(Throwable cause, String key, String operation) {
        if (cause instanceof IOException) {
            if (log.isWarnEnabled()) {
                log.warn("{} key={}: replica unavailable: {}", operation, key, cause.getMessage());
            }
        } else {
            log.error("{} key={}: unexpected error in replica task", operation, key, cause);
        }
    }

    /**
     * Результат одной задачи-реплики: успех с опциональным значением или провал.
     */
    record ReplicaTaskResult(boolean succeeded, Optional<VersionedEntry> value) {

        static ReplicaTaskResult success(Optional<VersionedEntry> value) {
            return new ReplicaTaskResult(true, value);
        }

        static ReplicaTaskResult failure() {
            return new ReplicaTaskResult(false, Optional.empty());
        }
    }
}
