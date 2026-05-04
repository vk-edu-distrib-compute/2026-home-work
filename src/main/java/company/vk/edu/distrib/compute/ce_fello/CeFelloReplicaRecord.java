package company.vk.edu.distrib.compute.ce_fello;

import java.util.Arrays;

record CeFelloReplicaRecord(long version, boolean tombstone, byte[] value) {
    CeFelloReplicaRecord {
        value = value == null ? new byte[0] : Arrays.copyOf(value, value.length);
    }

    static CeFelloReplicaRecord live(long version, byte[] value) {
        return new CeFelloReplicaRecord(version, false, value);
    }

    static CeFelloReplicaRecord tombstone(long version) {
        return new CeFelloReplicaRecord(version, true, new byte[0]);
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }
}
