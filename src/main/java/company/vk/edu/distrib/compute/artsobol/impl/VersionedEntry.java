package company.vk.edu.distrib.compute.artsobol.impl;

import java.util.Arrays;

record VersionedEntry(long version, boolean tombstone, byte[] value) {
    VersionedEntry {
        value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    static VersionedEntry value(long version, byte[] value) {
        return new VersionedEntry(version, false, value);
    }

    static VersionedEntry tombstone(long version) {
        return new VersionedEntry(version, true, null);
    }

    byte[] body() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    static boolean isNewer(VersionedEntry candidate, VersionedEntry current) {
        if (current == null) {
            return true;
        }
        if (candidate.version() != current.version()) {
            return candidate.version() > current.version();
        }
        return candidate.tombstone() && !current.tombstone();
    }
}
