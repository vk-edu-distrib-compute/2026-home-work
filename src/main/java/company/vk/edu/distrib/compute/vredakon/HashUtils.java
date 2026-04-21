package company.vk.edu.distrib.compute.vredakon;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HashUtils {
    private static final Logger log = LoggerFactory.getLogger(HashUtils.class);
    
    private HashUtils() {
        throw new UnsupportedOperationException();
    }

    public static long hashSHA256(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return hash & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException e) {
            log.error("Algorithm was not founded");
        }
        return 0;
    }
}
