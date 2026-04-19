package company.vk.edu.distrib.compute.wolfram158;

import java.security.NoSuchAlgorithmException;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface Router {
    String getNode(String key) throws NoSuchAlgorithmException;
}
