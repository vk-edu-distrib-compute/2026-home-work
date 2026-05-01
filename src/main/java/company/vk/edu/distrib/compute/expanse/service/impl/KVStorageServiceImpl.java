package company.vk.edu.distrib.compute.expanse.service.impl;

import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.dao.DaoAdapter;
import company.vk.edu.distrib.compute.expanse.dao.impl.DaoAdapterImpl;
import company.vk.edu.distrib.compute.expanse.service.KVStorageService;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class KVStorageServiceImpl implements KVStorageService<String, byte[]> {
    private final DaoAdapter<String, byte[]> daoAdapter;
    private final AtomicBoolean isOk;

    public KVStorageServiceImpl() {
        this.daoAdapter = AppContextUtils.getBean(DaoAdapterImpl.class);
        this.isOk = new AtomicBoolean(true);
    }

    @Override
    public boolean isOkStatus() {
        return isOk.get();
    }

    @Override
    public byte[] getEntityByID(String key) {
        return daoAdapter.get(key);
    }

    @Override
    public byte[] getEntityByID(String id, UUID replicaID) {
        return getEntityByID(resolveKey(id, replicaID));
    }

    @Override
    public void putEntity(String key, byte[] entity) {
        daoAdapter.save(key, entity);
    }

    @Override
    public void putEntity(String id, byte[] entity, UUID replicaID) {
        putEntity(resolveKey(id, replicaID), entity);
    }

    @Override
    public void deleteEntityByID(String key) {
        daoAdapter.delete(key);
    }

    @Override
    public void deleteEntityByID(String id, UUID replicaID) {
        deleteEntityByID(resolveKey(id, replicaID));
    }

    private String resolveKey(String id, UUID replicaID) {
        return id + ":" + replicaID.toString();
    }
}
