package company.vk.edu.distrib.compute.martinez1337.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

public class RendezvousSharding implements ShardingStrategy {
    @Override
    public int getResponsibleNode(String key, List<String> endpoints) {
        long maxWeight = Long.MIN_VALUE;
        int bestNode = 0;

        for (int i = 0; i < endpoints.size(); i++) {
            long weight = hash(key + endpoints.get(i));
            if (weight > maxWeight) {
                maxWeight = weight;
                bestNode = i;
            }
        }
        return bestNode;
    }

    @Override
    public long hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return ((long) (hash[0] & 0xff) << 24)
                    | ((long) (hash[1] & 0xff) << 16)
                    | ((long) (hash[2] & 0xff) << 8)
                    | (hash[3] & 0xff);
        } catch (Exception e) {
            return input.hashCode();
        }
    }
}
