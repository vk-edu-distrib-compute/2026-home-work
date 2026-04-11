package company.vk.edu.distrib.compute.handlest.storage;

public interface HandlestStorage<T> {
    T get(String key);

    void put(String key, T value);

    void remove(String key);

    void clear();
}
