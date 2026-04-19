package company.vk.edu.distrib.compute.bushuev_a_s;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MyRendezvousHashing implements MyHashingStrategy {
    @Override
    public String getEndpoint(String key, List<String> endpoints) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            BigInteger maxNumber = BigInteger.ZERO;
            int chosenNode = 0;

            for (int i = 0; i < endpoints.size(); i++) {
                String endpoint = endpoints.get(i);
                byte[] digest = md.digest((endpoint + key).getBytes(StandardCharsets.UTF_8));
                BigInteger hashNumber = new BigInteger(1, digest);

                if (hashNumber.compareTo(maxNumber) > 0) {
                    maxNumber = hashNumber;
                    chosenNode = i;
                }
            }
            return endpoints.get(chosenNode);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
