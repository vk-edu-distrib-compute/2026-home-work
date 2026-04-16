package company.vk.edu.distrib.compute.andeco.consistent_hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5HashFunction implements HashFunction<Long, String> {

    private static final ThreadLocal<MessageDigest> MD5 =
            ThreadLocal.withInitial(() -> {
                try {
                    return MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            });

    @Override
    public Long hash(String key) {
        MessageDigest md = MD5.get();
        md.reset();

        byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));

        return ByteBuffer.wrap(digest).getLong();
    }
}
