package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Реплицированное DAO: распределяет операции по репликам через ReplicaExecutor,
 * а результаты чтения собирает через ReplicaResultCollector.
 */
public class ReplicatedFileSystemDao implements Dao<byte[]> {

    private final ReplicaExecutor executor;
    private static final int DEFAULT_ACK = 1;

    public ReplicatedFileSystemDao(String baseDir, int n) throws IOException {
        this.executor = new ReplicaExecutor(baseDir, n);
    }

    @Override
    public byte[] get(String key) throws IOException, NoSuchElementException {
        return get(key, DEFAULT_ACK);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        upsert(key, value, DEFAULT_ACK);
    }

    @Override
    public void delete(String key) throws IOException {
        delete(key, DEFAULT_ACK);
    }

    public byte[] get(String key, int ack) throws IOException {
        return ReplicaResultCollector.collectReadResults(
                executor.submitReadTasks(key), ack);
    }

    public void upsert(String key, byte[] value, int ack) throws IOException {
        executor.upsert(key, value, ack);
    }

    public void delete(String key, int ack) throws IOException {
        executor.delete(key, ack);
    }

    public void disableReplica(int nodeId) {
        executor.disable(nodeId);
    }

    public void enableReplica(int nodeId) {
        executor.enable(nodeId);
    }

    public int getReplicaCount() {
        return executor.getReplicaCount();
    }

    public int getKeyCount(int replicaIndex) {
        return executor.getKeyCount(replicaIndex);
    }

    public int getReadAccessCount(int replicaIndex) {
        return executor.getReadAccessCount(replicaIndex);
    }

    public int getWriteAccessCount(int replicaIndex) {
        return executor.getWriteAccessCount(replicaIndex);
    }

    @Override
    public void close() throws IOException {
        executor.shutdownAndClose();
    }
}
