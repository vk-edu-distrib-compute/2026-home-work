package company.vk.edu.distrib.compute.maryarta.replication;

public class StoredRecord {
    private final byte[] data;
    private final long version;
    private final boolean deleted;

    public StoredRecord(byte[] data, long version, boolean deleted) {
        this.data = data;
        this.version = version;
        this.deleted = deleted;
    }

    public byte[] getData() {
        return data;
    }

    public long getVersion() {
        return version;
    }

    public boolean isDeleted() {
        return deleted;
    }
}