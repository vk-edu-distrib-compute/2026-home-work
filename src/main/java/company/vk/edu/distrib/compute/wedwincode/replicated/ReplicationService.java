package company.vk.edu.distrib.compute.wedwincode.replicated;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ReplicationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class ReplicationService {
    private final List<Dao<DaoRecord>> replicas;
    private final IntPredicate isEnabled;
    private final ExecutorService replicaExecutor;

    public record ReadResult(boolean responded, DaoRecord record) {}

    public record AggregationResult(int responded, DaoRecord best) {}

    public ReplicationService(List<Dao<DaoRecord>> replicas, IntPredicate isEnabled) {
        this.replicas = replicas;
        this.isEnabled = isEnabled;
        this.replicaExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public <T> CompletionService<T> createTasks(Function<Integer, T> task) {
        CompletionService<T> cs = new ExecutorCompletionService<>(replicaExecutor);
        for (int i = 0; i < replicas.size(); i++) {
            if (!isEnabled.test(i)) {
                continue;
            }
            final int replicaId = i;
            cs.submit(() -> task.apply(replicaId));
        }

        return cs;
    }

    public <T> List<T> getResults(CompletionService<T> cs, int ack, Predicate<T> isSuccess) {
        int submitted = countEnabledReplicas();
        int responded = 0;
        int completed = 0;
        List<T> results = new ArrayList<>();

        try {
            while (completed < submitted) {
                T result = cs.take().get();
                results.add(result);
                completed++;

                if (isSuccess.test(result)) {
                    responded++;
                    if (responded >= ack) {
                        break;
                    }
                }
                if (responded + (submitted - completed) < ack) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicationException("thread interrupted", e);
        } catch (ExecutionException e) {
            throw new ReplicationException("executor error", e);
        }

        return results;
    }

    public AggregationResult aggregateResults(List<ReadResult> results) {
        DaoRecord best = null;
        int responded = 0;
        for (ReadResult result: results) {
            if (result.responded()) {
                responded++;
            }

            DaoRecord current = result.record();
            if (current != null && (best == null || current.timestamp() > best.timestamp())) {
                best = current;
            }
        }

        return new AggregationResult(responded, best);
    }

    private int countEnabledReplicas() {
        int count = 0;
        for (int i = 0; i < replicas.size(); i++) {
            if (isEnabled.test(i)) {
                count++;
            }
        }
        return count;
    }
}
