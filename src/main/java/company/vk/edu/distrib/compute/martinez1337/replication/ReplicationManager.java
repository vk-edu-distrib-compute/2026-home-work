package company.vk.edu.distrib.compute.martinez1337.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.NoSuchElementException;

public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int REPLICA_ENABLED = 1;
    private static final int REPLICA_DISABLED = 0;

    private final List<Dao<byte[]>> replicas;
    private final AtomicIntegerArray enabled;
    private final ShardingStrategy sharding;
    private final AtomicLong clock = new AtomicLong();

    public ReplicationManager(List<Dao<byte[]>> replicas, ShardingStrategy sharding) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("Replicas list must not be empty");
        }
        this.sharding = sharding;
        this.replicas = List.copyOf(replicas);
        this.enabled = new AtomicIntegerArray(replicas.size());
        for (int i = 0; i < replicas.size(); i++) {
            this.enabled.set(i, REPLICA_ENABLED);
        }
    }

    public int replicasCount() {
        return replicas.size();
    }

    public void disableReplica(int replicaId) {
        validateReplicaId(replicaId);
        enabled.set(replicaId, REPLICA_DISABLED);
    }

    public void enableReplica(int replicaId) {
        validateReplicaId(replicaId);
        enabled.set(replicaId, REPLICA_ENABLED);
    }

    public List<Integer> selectReplicas(String key) {
        List<WeightedReplica> weights = new ArrayList<>(replicas.size());
        for (int i = 0; i < replicas.size(); i++) {
            long weight = sharding.hash(key + ":" + i);
            weights.add(new WeightedReplica(i, weight));
        }
        weights.sort((left, right) -> Long.compare(right.weight(), left.weight()));

        List<Integer> result = new ArrayList<>(replicas.size());
        for (WeightedReplica weightedReplica : weights) {
            result.add(weightedReplica.id());
        }
        return result;
    }

    public int upsert(String key, byte[] value, int ack, List<Integer> replicasForKey) {
        long version = nextVersion();
        int successful = 0;
        for (int replicaId : replicasForKey) {
            if (enabled.get(replicaId) == REPLICA_DISABLED) {
                log.debug("Replica {} is disabled, skip upsert", replicaId);
                continue;
            }
            try {
                replicas.get(replicaId).upsert(key, StoredValue.value(value, version).encode());
                successful++;
            } catch (IOException e) {
                log.warn("Failed to write replica {} for key {}", replicaId, key, e);
            }
        }
        log.info("PUT key={} ack={} successful={}", key, ack, successful);
        return successful >= ack ? 201 : 500;
    }

    public int delete(String key, int ack, List<Integer> replicasForKey) {
        long version = nextVersion();
        int successful = 0;
        for (int replicaId : replicasForKey) {
            if (enabled.get(replicaId) == REPLICA_DISABLED) {
                log.debug("Replica {} is disabled, skip delete", replicaId);
                continue;
            }
            try {
                replicas.get(replicaId).upsert(key, StoredValue.tombstone(version).encode());
                successful++;
            } catch (IOException e) {
                log.warn("Failed to delete (tombstone) on replica {} for key {}", replicaId, key, e);
            }
        }
        log.info("DELETE key={} ack={} successful={}", key, ack, successful);
        return successful >= ack ? 202 : 500;
    }

    public ReadResult get(String key, int ack, List<Integer> replicasForKey) {
        int successful = 0;
        StoredValue latest = null;

        for (int replicaId : replicasForKey) {
            if (enabled.get(replicaId) == REPLICA_DISABLED) {
                log.debug("Replica {} is disabled, skip read", replicaId);
                continue;
            }

            try {
                StoredValue current = tryRead(replicaId, key);
                successful++;
                latest = pickLatest(latest, current);
            } catch (NoSuchElementException e) {
                successful++;
            } catch (IOException e) {
                log.warn("Failed to read replica {} for key {}", replicaId, key, e);
            }
        }

        return buildReadResult(successful, ack, latest);
    }

    private StoredValue tryRead(int replicaId, String key) throws IOException {
        byte[] rawValue = replicas.get(replicaId).get(key);
        return StoredValue.decode(rawValue);
    }

    private StoredValue pickLatest(StoredValue a, StoredValue b) {
        if (a == null) return b;
        return b.version() > a.version() ? b : a;
    }

    private ReadResult buildReadResult(int successful, int ack, StoredValue latest) {
        if (successful < ack) {
            return new ReadResult(500, null);
        }
        if (latest == null || latest.tombstone()) {
            return new ReadResult(404, null);
        }
        return new ReadResult(200, latest.value());
    }

    private long nextVersion() {
        return clock.incrementAndGet();
    }

    private void validateReplicaId(int replicaId) {
        if (replicaId < 0 || replicaId >= replicas.size()) {
            throw new IllegalArgumentException("Replica id out of range: " + replicaId);
        }
    }

    public void close() throws IOException {
        IOException firstError = null;
        for (Dao<byte[]> dao : replicas) {
            try {
                dao.close();
            } catch (IOException e) {
                if (firstError == null) {
                    firstError = e;
                }
                log.warn("Failed to close replica DAO", e);
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }
}
