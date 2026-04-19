package company.vk.edu.distrib.compute.wolfram158;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Rendezvous implements Router {
    private static final BigInteger MIN = BigInteger.valueOf(Long.MAX_VALUE).pow(3).negate();
    private final List<String> endpoints;
    private final HashFunction hashFunction;

    public Rendezvous(List<String> endpoints) {
        this.endpoints = endpoints;
        this.hashFunction = new HashFunction();
    }

    @Override
    public String getNode(String key) throws NoSuchAlgorithmException {
        var maxHash = MIN;
        String result = null;
        for (String endpoint : endpoints) {
            final var hash = hashFunction.getHash(endpoint + key);
            if (hash.compareTo(maxHash) > 0) {
                maxHash = hash;
                result = endpoint;
            }
        }
        return result;
    }
}
