package company.vk.edu.distrib.compute.wedwincode.replicated;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

public class ParallelReplicationService {
    private final List<Dao<DaoRecord>> replicas;
    private final IntPredicate isEnabled;
    private final Executor replicaExecutor;

    public ParallelReplicationService(List<Dao<DaoRecord>> replicas, IntPredicate isEnabled) {
        this.replicas = replicas;
        this.isEnabled = isEnabled;
        this.replicaExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public <T> List<T> performTaskWithResult(Function<Integer, T> task) {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        for (int i = 0; i < replicas.size(); i++) {
            if (!isEnabled.test(i)) {
                continue;
            }

            final int replicaId = i;
            futures.add(CompletableFuture.supplyAsync(
                    () -> task.apply(replicaId),
                    replicaExecutor
            ));
        }

        return futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }
}
