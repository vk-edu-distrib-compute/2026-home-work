package company.vk.edu.distrib.compute.artsobol.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class ReplicaCommandExecutor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicaCommandExecutor.class);
    private static final long REPLICA_TIMEOUT_MS = 200L;

    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

    ReadResult read(String key, int ack, List<ReplicaCluster.Replica> orderedReplicas) {
        if (log.isDebugEnabled()) {
            log.debug("Replica read started: key={}, ack={}, replicas={}", key, ack, replicaIds(orderedReplicas));
        }

        List<ReadOutcome> outcomes = readOutcomes(key, orderedReplicas);
        int successfulResponses = successfulResponses(outcomes);
        VersionedEntry freshest = freshestEntry(outcomes);
        List<ReplicaCluster.Replica> responsiveReplicas = responsiveReplicas(outcomes);

        if (successfulResponses >= ack && freshest != null) {
            repair(key, freshest, responsiveReplicas);
            if (log.isDebugEnabled()) {
                log.debug("Replica read finished: key={}, ack={}, successes={}", key, ack, successfulResponses);
            }
        } else if (successfulResponses < ack && log.isWarnEnabled()) {
            log.warn(
                    "Replica read quorum not reached: key={}, ack={}, successes={}",
                    key,
                    ack,
                    successfulResponses
            );
        }

        return new ReadResult(successfulResponses, freshest);
    }

    int replicate(String key, VersionedEntry entry, List<ReplicaCluster.Replica> orderedReplicas) {
        String operation = entry.tombstone() ? "delete" : "put";
        if (log.isDebugEnabled()) {
            log.debug("Replica {} started: key={}, replicas={}", operation, key, replicaIds(orderedReplicas));
        }

        List<CompletableFuture<Boolean>> futures = new ArrayList<>(orderedReplicas.size());
        for (ReplicaCluster.Replica replica : orderedReplicas) {
            futures.add(writeFuture(replica, key, entry, true));
        }

        int successfulWrites = 0;
        for (CompletableFuture<Boolean> future : futures) {
            if (future.join()) {
                successfulWrites++;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Replica {} finished: key={}, successes={}", operation, key, successfulWrites);
        }
        return successfulWrites;
    }

    private void repair(String key, VersionedEntry freshest, List<ReplicaCluster.Replica> responsiveReplicas) {
        List<CompletableFuture<Boolean>> repairs = new ArrayList<>(responsiveReplicas.size());
        for (ReplicaCluster.Replica replica : responsiveReplicas) {
            repairs.add(writeFuture(replica, key, freshest, false));
        }
        repairs.forEach(CompletableFuture::join);
    }

    private CompletableFuture<ReadOutcome> readFuture(ReplicaCluster.Replica replica, String key) {
        return CompletableFuture
                .supplyAsync(() -> {
                    try {
                        return new ReadOutcome(replica, replica.read(key), true);
                    } catch (IOException e) {
                        return new ReadOutcome(replica, null, false);
                    }
                }, ioExecutor)
                .orTimeout(REPLICA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(error -> {
                    if (log.isWarnEnabled()) {
                        log.warn(
                                "Replica read failed: key={}, replicaId={}, reason={}",
                                key,
                                replica.id(),
                                error.toString()
                        );
                    }
                    return new ReadOutcome(replica, null, false);
                });
    }

    private CompletableFuture<Boolean> writeFuture(
            ReplicaCluster.Replica replica,
            String key,
            VersionedEntry entry,
            boolean countAccess
    ) {
        String operation = entry.tombstone() ? "delete" : "put";
        return CompletableFuture
                .supplyAsync(() -> {
                    try {
                        return replica.writeIfNewer(key, entry, countAccess);
                    } catch (IOException e) {
                        return false;
                    }
                }, ioExecutor)
                .orTimeout(REPLICA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(error -> {
                    if (log.isWarnEnabled()) {
                        log.warn(
                                "Replica {} failed: key={}, replicaId={}, reason={}",
                                operation,
                                key,
                                replica.id(),
                                error.toString()
                        );
                    }
                    return false;
                });
    }

    private List<ReadOutcome> readOutcomes(String key, List<ReplicaCluster.Replica> orderedReplicas) {
        List<CompletableFuture<ReadOutcome>> futures = new ArrayList<>(orderedReplicas.size());
        for (ReplicaCluster.Replica replica : orderedReplicas) {
            futures.add(readFuture(replica, key));
        }

        List<ReadOutcome> outcomes = new ArrayList<>(futures.size());
        for (CompletableFuture<ReadOutcome> future : futures) {
            outcomes.add(future.join());
        }
        return outcomes;
    }

    private static int successfulResponses(List<ReadOutcome> outcomes) {
        int successfulResponses = 0;
        for (ReadOutcome outcome : outcomes) {
            if (outcome.success()) {
                successfulResponses++;
            }
        }
        return successfulResponses;
    }

    private static VersionedEntry freshestEntry(List<ReadOutcome> outcomes) {
        VersionedEntry freshest = null;
        for (ReadOutcome outcome : outcomes) {
            if (!outcome.success()) {
                continue;
            }
            VersionedEntry current = outcome.entry();
            if (current != null && VersionedEntry.isNewer(current, freshest)) {
                freshest = current;
            }
        }
        return freshest;
    }

    private static List<ReplicaCluster.Replica> responsiveReplicas(List<ReadOutcome> outcomes) {
        List<ReplicaCluster.Replica> responsiveReplicas = new ArrayList<>(outcomes.size());
        for (ReadOutcome outcome : outcomes) {
            if (outcome.success()) {
                responsiveReplicas.add(outcome.replica());
            }
        }
        return responsiveReplicas;
    }

    private static String replicaIds(List<ReplicaCluster.Replica> replicas) {
        List<Integer> ids = new ArrayList<>(replicas.size());
        for (ReplicaCluster.Replica replica : replicas) {
            ids.add(replica.id());
        }
        return ids.toString();
    }

    @Override
    public void close() {
        ioExecutor.close();
    }

    private record ReadOutcome(ReplicaCluster.Replica replica, VersionedEntry entry, boolean success) {
    }
}
