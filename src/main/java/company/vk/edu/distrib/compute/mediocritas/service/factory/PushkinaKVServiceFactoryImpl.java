package company.vk.edu.distrib.compute.mediocritas.service.factory;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.mediocritas.service.StandaloneKvByteService;
import company.vk.edu.distrib.compute.mediocritas.storage.FileByteDao;

import java.io.IOException;

public class PushkinaKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) {
        try {
            return new StandaloneKvByteService(port, new FileByteDao());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create DAO", e);
        }
    }
}
