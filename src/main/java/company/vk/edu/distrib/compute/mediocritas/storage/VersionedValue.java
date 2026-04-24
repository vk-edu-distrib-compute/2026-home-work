package company.vk.edu.distrib.compute.mediocritas.storage;

import java.util.Arrays;

public record VersionedValue(byte[] data, long timestamp, boolean tombstone) {
    public VersionedValue(byte[] data, long timestamp) {
        this(data, timestamp, false);
    }

    public static VersionedValue deleted(long timestamp) {
        return new VersionedValue(null, timestamp, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VersionedValue that = (VersionedValue) o;
        return timestamp == that.timestamp && tombstone == that.tombstone && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Boolean.hashCode(tombstone);
        return result;
    }
}
