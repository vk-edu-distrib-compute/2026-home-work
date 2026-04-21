package company.vk.edu.distrib.compute.ce_fello;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class CeFelloHashing {
    private CeFelloHashing() {
    }

    static long hash64(String value) {
        return hash64(value.getBytes(StandardCharsets.UTF_8));
    }

    static long hash64(byte[] value) {
        MessageDigest messageDigest = messageDigest();
        byte[] digest = messageDigest.digest(value);
        return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
    }

    private static MessageDigest messageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
