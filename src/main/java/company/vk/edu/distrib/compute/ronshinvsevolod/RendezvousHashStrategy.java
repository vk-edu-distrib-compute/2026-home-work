package company.vk.edu.distrib.compute.ronshinvsevolod;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

public class RendezvousHashStrategy implements HashStrategy {
    private final List<String> endpoints;

    public RendezvousHashStrategy(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public String getEndpoint(String key) {
        long maxHash = Long.MIN_VALUE;
        String selected = null;
        for (String endpoint : endpoints) {
            long hash = hash(endpoint + key);
            if (hash > maxHash) {
                maxHash = hash;
                selected = endpoint;
            }
        }
        return selected;
    }

    private long hash(String s) {
        CRC32 crc = new CRC32();
        crc.update(s.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }
}
