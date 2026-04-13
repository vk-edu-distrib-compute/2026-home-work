package company.vk.edu.distrib.compute.wedwincode.sharded;

import java.util.List;
import java.util.zip.CRC32;

public class RendezvousHashStrategy implements HashStrategy {
    private final List<String> endpoints;

    public RendezvousHashStrategy(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new RuntimeException("endpoints list is null or empty");
        }
        this.endpoints = endpoints;
    }

    @Override
    public String getEndpoint(String id) {
        long bestScore = Long.MIN_VALUE;
        String bestEndpoint = null;

        for (String endpoint: endpoints) {
            long score = hash(id + endpoint);
            if (score > bestScore) {
                bestEndpoint = endpoint;
                bestScore = score;
            }
        }

        return bestEndpoint;
    }

    private long hash(String input) {
        CRC32 crc = new CRC32();
        crc.update(input.getBytes());
        return crc.getValue();
    }
}
