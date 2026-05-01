package company.vk.edu.distrib.compute.expanse.service;

import java.util.UUID;

public interface KVStorageService<K, T> {
    boolean isOkStatus();

    T getEntityByID(K id);

    T getEntityByID(K id, UUID replicaID);

    void putEntity(K id, T entity);

    void putEntity(K id, T entity, UUID replicaID);

    void deleteEntityByID(K id);

    void deleteEntityByID(K id, UUID replicaID);
}
