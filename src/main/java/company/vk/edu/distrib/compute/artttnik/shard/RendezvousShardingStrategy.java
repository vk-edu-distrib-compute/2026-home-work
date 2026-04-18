package company.vk.edu.distrib.compute.artttnik.shard;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

public class RendezvousShardingStrategy implements ShardingStrategy {

    @Override
    public String resolveOwner(String key, List<String> endpoints) {
        return endpoints.stream()
                .max(Comparator.comparingLong(endpoint -> score(key, endpoint)))
                .orElseThrow(() -> new IllegalArgumentException("Endpoint list is empty"));
    }

    private static long score(String key, String endpoint) {
        byte[] bytes = (key + '#' + endpoint).getBytes(StandardCharsets.UTF_8);
        long hash = 0xcbf29ce484222325L;
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
