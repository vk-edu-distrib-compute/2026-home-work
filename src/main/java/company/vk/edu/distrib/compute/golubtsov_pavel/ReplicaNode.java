package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaNode {
    private final int id;
    private final Map<String, ReplicaRecord> storage;
    private volatile boolean enabled;

    public ReplicaNode(int id) {
        this.id = id;
        this.storage = new ConcurrentHashMap<>();
        this.enabled = true;
    }

    public int getId() {
        return id;
    }

    public ReplicaRecord getRecord(String key) {
        validateKey(key);
        return storage.get(key);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void disable() {
        this.enabled = false;
    }

    public void enable() {
        this.enabled = true;
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

