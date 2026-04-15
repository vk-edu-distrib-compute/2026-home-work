package company.vk.edu.distrib.compute.wolfram158;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunction {
    private final MessageDigest md;

    public HashFunction() throws NoSuchAlgorithmException {
        this.md = MessageDigest.getInstance("MD5");
    }

    public BigInteger getHash(String key) {
        final byte[] hashBytes = md.digest(key.getBytes(StandardCharsets.UTF_8));
        final StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return new BigInteger(sb.toString(), 16);
    }
}
