package company.vk.edu.distrib.compute.vitos23.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashUtils {
    private static final String MD5_ALGORITHM = "MD5";

    private HashUtils() {
    }

    public static long md5Hash(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance(MD5_ALGORITHM);
            byte[] digest = md.digest(data.getBytes(StandardCharsets.UTF_8));
            return bytesToLong(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Hash algorithm not available: " + MD5_ALGORITHM, e);
        }
    }

    /// Create `long` value from <=8 first bytes
    private static long bytesToLong(byte[] digest) {
        long hash = 0;
        for (int i = 0; i < Math.min(8, digest.length); i++) {
            hash = (hash << 8) | (digest[i] & 0xFF);
        }
        return hash;
    }
}
