package company.vk.edu.distrib.compute.expanse.dao.impl;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.expanse.dao.DaoAdapter;
import company.vk.edu.distrib.compute.expanse.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.expanse.utils.ExceptionUtils;

import java.io.IOException;
import java.util.NoSuchElementException;

public class DaoAdapterImpl implements DaoAdapter<String, byte[]> {

    @Override
    public void save(String key, byte[] content) {
        try (Dao<byte[]> dao = new FileStorageDao()) {
            dao.upsert(key, content);

        } catch (IOException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    @Override
    public byte[] get(String key) {
        try (Dao<byte[]> dao = new FileStorageDao()) {
            return dao.get(key);

        } catch (IOException e) {
            throw ExceptionUtils.wrapToInternal(e);

        } catch (NoSuchElementException e) {
            throw new EntityNotFoundException(getEntityNotFoundMessage(key), e);
        }
    }

    @Override
    public void delete(String key) {
        try (Dao<byte[]> dao = new FileStorageDao()) {
            dao.delete(key);

        } catch (IOException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    private String getEntityNotFoundMessage(String key) {
        return String.format("Entity with key=%s is not found.", key);
    }
}
