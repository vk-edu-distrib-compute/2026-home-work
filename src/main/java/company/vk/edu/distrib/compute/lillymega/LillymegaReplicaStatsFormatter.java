package company.vk.edu.distrib.compute.lillymega;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

final class LillymegaReplicaStatsFormatter {
    String replicaStats(int replicaId, boolean available, Map<String, LillymegaVersionedEntry> replica) {
        long activeKeys = replica.values().stream()
                .filter(entry -> !entry.deleted())
                .count();
        int payloadBytes = replica.values().stream()
                .filter(entry -> !entry.deleted())
                .mapToInt(entry -> entry.value().length)
                .sum();

        return "{"
                + "\"replicaId\":" + replicaId + ","
                + "\"available\":" + available + ","
                + "\"entries\":" + replica.size() + ","
                + "\"activeKeys\":" + activeKeys + ","
                + "\"payloadBytes\":" + payloadBytes
                + "}";
    }

    String replicaAccessStats(int replicaId, AtomicLong readAccess, AtomicLong writeAccess) {
        return "{"
                + "\"replicaId\":" + replicaId + ","
                + "\"reads\":" + readAccess.get() + ","
                + "\"writes\":" + writeAccess.get()
                + "}";
    }
}
