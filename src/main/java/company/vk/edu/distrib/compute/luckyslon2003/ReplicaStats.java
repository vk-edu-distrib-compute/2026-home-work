package company.vk.edu.distrib.compute.luckyslon2003;

import java.util.concurrent.atomic.AtomicLong;

final class ReplicaStats {
    private final AtomicLong storedKeys = new AtomicLong();
    private final AtomicLong tombstones = new AtomicLong();
    private final AtomicLong storedBytes = new AtomicLong();
    private final AtomicLong readRequests = new AtomicLong();
    private final AtomicLong writeRequests = new AtomicLong();
    private final AtomicLong deleteRequests = new AtomicLong();

    long storedKeys() {
        return storedKeys.get();
    }

    long tombstones() {
        return tombstones.get();
    }

    long storedBytes() {
        return storedBytes.get();
    }

    long readRequests() {
        return readRequests.get();
    }

    long writeRequests() {
        return writeRequests.get();
    }

    long deleteRequests() {
        return deleteRequests.get();
    }

    void recordRead() {
        readRequests.incrementAndGet();
    }

    void recordWrite() {
        writeRequests.incrementAndGet();
    }

    void recordDelete() {
        deleteRequests.incrementAndGet();
    }

    void adjustCounts(VersionedEntry previousEntry, VersionedEntry newEntry) {
        if (previousEntry != null) {
            if (previousEntry.tombstone()) {
                tombstones.decrementAndGet();
            } else {
                storedKeys.decrementAndGet();
                storedBytes.addAndGet(-previousEntry.valueSize());
            }
        }

        if (newEntry.tombstone()) {
            tombstones.incrementAndGet();
        } else {
            storedKeys.incrementAndGet();
            storedBytes.addAndGet(newEntry.valueSize());
        }
    }
}
