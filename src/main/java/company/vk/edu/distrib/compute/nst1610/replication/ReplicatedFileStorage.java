package company.vk.edu.distrib.compute.nst1610.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nst1610.dao.FileDao;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedFileStorage {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedFileStorage.class);

    private final List<Dao<byte[]>> replicas;
    private boolean[] enabledReplicas;
    private long versionClock = System.currentTimeMillis();

    public ReplicatedFileStorage(Path storagePath, int numberOfReplicas) throws IOException {
        this.replicas = new ArrayList<>(numberOfReplicas);
        for (int replicaId = 0; replicaId < numberOfReplicas; replicaId++) {
            replicas.add(new FileDao(storagePath.resolve("replica-" + replicaId)));
        }
        this.enabledReplicas = new boolean[numberOfReplicas];
        for (int replicaId = 0; replicaId < numberOfReplicas; replicaId++) {
            enabledReplicas[replicaId] = true;
        }
    }

    public int numberOfReplicas() {
        return replicas.size();
    }

    public void disableReplica(int replicaId) {
        validateReplicaId(replicaId);
        enabledReplicas[replicaId] = false;
        log.info("Replica {} disabled", replicaId);
    }

    public void enableReplica(int replicaId) {
        validateReplicaId(replicaId);
        enabledReplicas[replicaId] = true;
        log.info("Replica {} enabled", replicaId);
    }

    public boolean put(String key, byte[] value, int ack) throws IOException {
        return replicateWrite(key, new StoredValue(nextVersion(), false, value), ack, "PUT");
    }

    public boolean delete(String key, int ack) throws IOException {
        return replicateWrite(key, new StoredValue(nextVersion(), true, new byte[0]), ack, "DELETE");
    }

    public ReadResult get(String key, int ack) throws IOException {
        int successfulReads = 0;
        StoredValue latestValue = null;
        List<ReplicaSnapshot> snapshots = new ArrayList<>(replicas.size());
        for (int replicaId : replicaOrder(key)) {
            if (!isReplicaEnabled(replicaId)) {
                log.info("GET skipped disabled replica={} key={}", replicaId, key);
                continue;
            }
            try {
                StoredValue currentValue = StoredValue.decode(replicas.get(replicaId).get(key));
                snapshots.add(new ReplicaSnapshot(replicaId, currentValue));
                successfulReads++;
                latestValue = chooseLatest(latestValue, currentValue);
            } catch (NoSuchElementException e) {
                snapshots.add(new ReplicaSnapshot(replicaId, null));
                successfulReads++;
            } catch (IOException e) {
                log.error("GET failed on replica={} key={}", replicaId, key, e);
            }
        }
        log.info("GET key={} ack={} successfulReads={}", key, ack, successfulReads);
        if (successfulReads < ack) {
            return ReadResult.notEnoughReplicas();
        }
        if (latestValue == null) {
            return ReadResult.notFound();
        }
        updateReplicas(key, latestValue, snapshots);
        if (latestValue.tombstone()) {
            return ReadResult.notFound();
        }
        return ReadResult.found(latestValue.value());
    }

    private boolean replicateWrite(String key, StoredValue value, int ack, String operation) throws IOException {
        int successfulWrites = 0;
        byte[] encodedValue = value.encode();
        for (int replicaId : replicaOrder(key)) {
            if (!isReplicaEnabled(replicaId)) {
                log.info("{} skipped disabled replica={} key={}", operation, replicaId, key);
                continue;
            }
            try {
                replicas.get(replicaId).upsert(key, encodedValue);
                successfulWrites++;
            } catch (IOException e) {
                log.error("{} failed on replica={} key={}", operation, replicaId, key, e);
            }
        }
        log.info("{} key={} ack={} successfulWrites={}", operation, key, ack, successfulWrites);
        return successfulWrites >= ack;
    }

    private void updateReplicas(String key, StoredValue latestValue, List<ReplicaSnapshot> snapshots) {
        byte[] encodedLatestValue = null;
        for (ReplicaSnapshot snapshot : snapshots) {
            if (snapshot.value() != null && snapshot.value().timestamp() >= latestValue.timestamp()) {
                continue;
            }
            try {
                if (encodedLatestValue == null) {
                    encodedLatestValue = latestValue.encode();
                }
                replicas.get(snapshot.replicaId()).upsert(key, encodedLatestValue);
                log.info("Updated replica={} key={}", snapshot.replicaId(), key);
            } catch (IOException e) {
                log.error("Updated failed on replica={} key={}", snapshot.replicaId(), key, e);
            }
        }
    }

    private List<Integer> replicaOrder(String key) {
        return java.util.stream.IntStream.range(0, replicas.size())
            .boxed()
            .sorted(Comparator.comparingLong((Integer replicaId) -> hash(key, replicaId)).reversed())
            .toList();
    }

    private static long hash(String key, int replicaId) {
        return (key + ":" + replicaId).hashCode();
    }

    private static StoredValue chooseLatest(StoredValue left, StoredValue right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        if (left.timestamp() != right.timestamp()) {
            return left.timestamp() >= right.timestamp() ? left : right;
        }
        return left.tombstone() ? left : right;
    }

    private boolean isReplicaEnabled(int replicaId) {
        return enabledReplicas[replicaId];
    }

    private long nextVersion() {
        versionClock++;
        return versionClock;
    }

    private void validateReplicaId(int replicaId) {
        if (replicaId < 0 || replicaId >= replicas.size()) {
            throw new IllegalArgumentException();
        }
    }

    private record ReplicaSnapshot(int replicaId, StoredValue value) {
    }

    public record ReadResult(boolean enoughReplicas, byte[] value, boolean found) {
        public static ReadResult found(byte[] value) {
            return new ReadResult(true, value, true);
        }

        public static ReadResult notFound() {
            return new ReadResult(true, new byte[0], false);
        }

        public static ReadResult notEnoughReplicas() {
            return new ReadResult(false, new byte[0], false);
        }
    }
}
