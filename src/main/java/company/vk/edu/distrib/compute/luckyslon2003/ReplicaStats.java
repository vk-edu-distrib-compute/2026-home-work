package company.vk.edu.distrib.compute.luckyslon2003;

import java.util.concurrent.atomic.AtomicLong;

final class ReplicaStats {
    private final AtomicLong storedKeysCounter = new AtomicLong();
    private final AtomicLong tombstonesCounter = new AtomicLong();
    private final AtomicLong storedBytesCounter = new AtomicLong();
    private final AtomicLong readRequestsCounter = new AtomicLong();
    private final AtomicLong writeRequestsCounter = new AtomicLong();
    private final AtomicLong deleteRequestsCounter = new AtomicLong();

    long storedKeys() {
        return storedKeysCounter.get();
    }

    long tombstones() {
        return tombstonesCounter.get();
    }

    long storedBytes() {
        return storedBytesCounter.get();
    }

    long readRequests() {
        return readRequestsCounter.get();
    }

    long writeRequests() {
        return writeRequestsCounter.get();
    }

    long deleteRequests() {
        return deleteRequestsCounter.get();
    }

    void recordRead() {
        readRequestsCounter.incrementAndGet();
    }

    void recordWrite() {
        writeRequestsCounter.incrementAndGet();
    }

    void recordDelete() {
        deleteRequestsCounter.incrementAndGet();
    }

    void adjustCounts(VersionedEntry previousEntry, VersionedEntry newEntry) {
        if (previousEntry != null) {
            if (previousEntry.tombstone()) {
                tombstonesCounter.decrementAndGet();
            } else {
                storedKeysCounter.decrementAndGet();
                storedBytesCounter.addAndGet(-previousEntry.valueSize());
            }
        }

        if (newEntry.tombstone()) {
            tombstonesCounter.incrementAndGet();
        } else {
            storedKeysCounter.incrementAndGet();
            storedBytesCounter.addAndGet(newEntry.valueSize());
        }
    }
}
