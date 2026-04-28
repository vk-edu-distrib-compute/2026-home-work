package company.vk.edu.distrib.compute.usl.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class HashSupport {
    private static final byte DELIMITER = 0;
    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(HashSupport::createMd5);

    private HashSupport() {
    }

    static long hash64(String... parts) {
        MessageDigest digest = MD5.get();
        digest.reset();
        for (String part : parts) {
            digest.update(part.getBytes(StandardCharsets.UTF_8));
            digest.update(DELIMITER);
        }
        return ByteBuffer.wrap(digest.digest()).getLong();
    }

    private static MessageDigest createMd5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("MD5 is not available", e);
        }
    }
}
