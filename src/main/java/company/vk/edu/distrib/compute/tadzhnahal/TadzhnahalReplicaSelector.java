package company.vk.edu.distrib.compute.tadzhnahal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

public class TadzhnahalReplicaSelector {
    private final int replicaCount;

    public TadzhnahalReplicaSelector(int replicaCount) {
        if (replicaCount < 1) {
            throw new IllegalArgumentException("Replica count must be positive");
        }

        this.replicaCount = replicaCount;
    }

    public List<Integer> select(String key, int count) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }

        if (count < 1 || count > replicaCount) {
            throw new IllegalArgumentException("Invalid replica selection count");
        }

        List<Integer> replicaIds = new ArrayList<>(replicaCount);
        for (int nodeId = 0; nodeId < replicaCount; nodeId++) {
            replicaIds.add(nodeId);
        }

        replicaIds.sort(
                Comparator.comparingLong((Integer nodeId) -> score(key, nodeId))
                        .reversed()
                        .thenComparingInt(Integer::intValue)
        );

        return List.copyOf(replicaIds.subList(0, count));
    }

    private long score(String key, int nodeId) {
        CRC32 crc32 = new CRC32();
        String value = key + "#" + nodeId;
        crc32.update(value.getBytes(StandardCharsets.UTF_8));
        return crc32.getValue();
    }
}
