package company.vk.edu.distrib.compute.expanse.dao;

public interface DaoAdapter<K, T> {
    void save(K key, T content);

    T get(K key);

    void delete(K key);
}
