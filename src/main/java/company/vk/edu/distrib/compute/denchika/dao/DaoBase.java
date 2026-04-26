package company.vk.edu.distrib.compute.denchika.dao;

public abstract class DaoBase {

    protected static final int MAX_SIZE = 1024 * 1024;

    protected static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Invalid key");
        }
    }
}
