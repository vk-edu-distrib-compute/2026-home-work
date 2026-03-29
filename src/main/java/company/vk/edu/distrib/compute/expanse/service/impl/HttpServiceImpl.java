package company.vk.edu.distrib.compute.expanse.service.impl;

import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.dao.DaoAdapter;
import company.vk.edu.distrib.compute.expanse.dao.impl.DaoAdapterImpl;
import company.vk.edu.distrib.compute.expanse.service.HttpService;

import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServiceImpl implements HttpService<String, byte[]> {
    private final DaoAdapter<String, byte[]> daoAdapter;
    private final AtomicBoolean isOk;

    public HttpServiceImpl() {
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
    public void putEntity(String key, byte[] entity) {
        daoAdapter.save(key, entity);
    }

    @Override
    public void deleteEntityByID(String key) {
        daoAdapter.delete(key);
    }
}
