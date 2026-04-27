package company.vk.edu.distrib.compute.maryarta.replication;

import java.util.Arrays;

public record StoredRecord(byte[] data, long version, boolean deleted) {
    public StoredRecord(byte[] data, long version, boolean deleted) {
        this.data = data == null ? null : Arrays.copyOf(data, data.length);
        this.version = version;
        this.deleted = deleted;
    }

    @Override
    public byte[] data() {
        return data == null ? null : Arrays.copyOf(data, data.length);
    }
}
