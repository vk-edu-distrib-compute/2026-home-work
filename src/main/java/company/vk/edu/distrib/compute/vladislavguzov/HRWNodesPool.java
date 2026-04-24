package company.vk.edu.distrib.compute.vladislavguzov;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HRWNodesPool implements NodesRouter {

    private final Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();

    private long hash(String key, String nodeId) throws NoSuchAlgorithmException {
        try {
            byte[] digest = MessageDigest.getInstance("MD5")
                    .digest((key + "#" + nodeId).getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h |= ((long) (digest[i] & 0xFF)) << (i * 8);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new NoSuchAlgorithmException("Unsupported algorithm", e);
        }
    }

    @Override
    public void add(ClusterNode node, String id) {
        nodes.put(id, node);
    }

    @Override
    public void remove(String id) {
        nodes.remove(id);
    }

    @Override
    public ClusterNode get(String key) throws NoSuchAlgorithmException {
        if (nodes.isEmpty()) {
            return null;
        }

        ClusterNode winner = null;
        long maxHash = Long.MIN_VALUE;

        for (Map.Entry<String, ClusterNode> entry : nodes.entrySet()) {
            long h = hash(key, entry.getKey());
            if (winner == null || h > maxHash) {
                maxHash = h;
                winner = entry.getValue();
            }
        }

        return winner;
    }

}
