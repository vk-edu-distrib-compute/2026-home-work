package company.vk.edu.distrib.compute.bushuev_a_s;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MyConsistentHashing implements MyHashingStrategy {
    private static final int VIRTUAL_NODES = 5;
    private final NavigableMap<Long, String> circle = new TreeMap<>();

    @Override
    public String getEndpoint(String key, List<String> endpoints) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");

        for (String node : endpoints) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                byte[] digest = md.digest((node + ":" + i).getBytes(StandardCharsets.UTF_8));
                circle.put(ByteBuffer.wrap(digest).getLong(), node);
            }
        }

        byte[] keyDigest = md.digest(key.getBytes(StandardCharsets.UTF_8));
        long keyHash = ByteBuffer.wrap(keyDigest).getLong();

        var entry = circle.ceilingEntry(keyHash);

        return (entry != null) ? entry.getValue() : circle.firstEntry().getValue();
    }
}
