package company.vk.edu.distrib.compute.bushuev_a_s;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MyRendezvousHashing implements MyHashingStrategy {
    @Override
    public String getEndpoint(String key, List<String> endpoints) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");

        Long maxHash = Long.MIN_VALUE;

        int chosenNode = 0;

        for (int i = 0; i < endpoints.size(); i++) {
            String combined = endpoints.get(i) + key;
            byte[] digest = md.digest(combined.getBytes(StandardCharsets.UTF_8));

            // Берем первые 8 байт из 32-байтного SHA-256 и превращаем их в long
            long currentHash = ByteBuffer.wrap(digest).getLong();

            if (currentHash > maxHash) {
                maxHash = currentHash;
                chosenNode = i;
            }
        }
        return endpoints.get(chosenNode);
    }
}
