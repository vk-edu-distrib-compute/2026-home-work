package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

public class RendezvousHashing implements DistributingAlgorithm {

    @Override
    public String selectNode(String key, List<String> nodes) {

        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        long bestScore = Long.MIN_VALUE;
        String bestNode = null;

        for (String node : nodes) {
            long score = hash(key + "|" + node);
            if (score > bestScore) {
                bestScore = score;
                bestNode = node;
            }
        }

        return bestNode;
    }

    private long hash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return java.nio.ByteBuffer.wrap(d).getLong();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
