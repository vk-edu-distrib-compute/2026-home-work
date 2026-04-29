package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaNode {
    private final int id;
    private final Map<String, ReplicaRecord> storage;
    private final AtomicBoolean enabled;

    public ReplicaNode(int id) {
        this.id = id;
        this.storage = new ConcurrentHashMap<>();
        this.enabled = new AtomicBoolean(true);
    }

    public int getId() {
        return id;
    }

    public ReplicaRecord getRecord(String key) {
        validateKey(key);
        return storage.get(key);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void disable() {
        enabled.set(false);
    }

    public void enable() {
        enabled.set(true);
    }

    public void upsert(String key, ReplicaRecord record) {
        validateKey(key);
        if (record == null) {
            throw new IllegalArgumentException("record is null");
        }
        storage.put(key, record);
    }

    public void delete(String key, ReplicaRecord tombstone) {
        validateKey(key);
        if (tombstone == null) {
            throw new IllegalArgumentException("tombstone is null");
        }
        storage.put(key, tombstone);
    }

    public static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
    }
}

