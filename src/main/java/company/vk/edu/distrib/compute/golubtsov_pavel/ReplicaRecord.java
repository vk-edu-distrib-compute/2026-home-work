package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.util.Arrays;

public class ReplicaRecord {
    private final byte[] value;
    private final long timestamp;
    private final boolean deleted;


    public ReplicaRecord(byte[] value, long timestamp, boolean deleted) {
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    public byte[] getValue() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    public boolean isDeleted() {
        return deleted;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
