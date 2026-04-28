package company.vk.edu.distrib.compute.usl;

import java.util.Arrays;

final class VersionedValue {
    private final long version;
    private final boolean tombstone;
    private final byte[] body;

    VersionedValue(long version, boolean tombstone, byte[] body) {
        this.version = version;
        this.tombstone = tombstone;
        this.body = body == null ? null : Arrays.copyOf(body, body.length);
    }

    static VersionedValue value(long version, byte[] body) {
        if (body == null) {
            throw new IllegalArgumentException("Value body must not be null");
        }
        return new VersionedValue(version, false, body);
    }

    static VersionedValue tombstone(long version) {
        return new VersionedValue(version, true, null);
    }

    long version() {
        return version;
    }

    boolean tombstone() {
        return tombstone;
    }

    byte[] body() {
        return body == null ? null : Arrays.copyOf(body, body.length);
    }

    int bodyLength() {
        return body == null ? 0 : body.length;
    }

    static boolean isNewer(VersionedValue candidate, VersionedValue current) {
        if (candidate == null) {
            return false;
        }
        if (current == null) {
            return true;
        }
        if (candidate.version != current.version) {
            return candidate.version > current.version;
        }
        return candidate.tombstone && !current.tombstone;
    }
}
