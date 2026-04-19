package company.vk.edu.distrib.compute.bushuev_a_s;

import java.security.NoSuchAlgorithmException;
import java.util.List;

@FunctionalInterface
public interface MyHashingStrategy {
    String getEndpoint(String key, List<String> endpoints) throws NoSuchAlgorithmException;
}
