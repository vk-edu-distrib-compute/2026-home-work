package company.vk.edu.distrib.compute.nihuaway00.replication;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

public class ReplicaSelector {
    private static final ThreadLocal<MessageDigest> MD =
            ThreadLocal.withInitial(ReplicaSelector::createMD5);

    public List<ReplicaNode> select(String key, List<ReplicaNode> replicas) {
        return replicas.stream()
                .sorted(Comparator.comparingLong(r -> -score(key, r.getNodeId())))
                .toList();
    }

    private long score(String key, int nodeId) {
        byte[] hash = MD.get().digest((key + "\0" + nodeId).getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(hash).getLong();
    }

    private static MessageDigest createMD5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
