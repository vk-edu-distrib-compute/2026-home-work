package company.vk.edu.distrib.compute.expanse.service;

public interface HttpService<K, T> {
    boolean isOkStatus();

    T getEntityByID(K id);

    void putEntity(K id, T entity);

    void deleteEntityByID(K id);
}
