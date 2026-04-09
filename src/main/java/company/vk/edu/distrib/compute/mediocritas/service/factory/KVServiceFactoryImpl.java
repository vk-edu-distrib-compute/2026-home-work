package company.vk.edu.distrib.compute.mediocritas.service.factory;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.mediocritas.service.InMemoryKvService;
import company.vk.edu.distrib.compute.mediocritas.storage.FileByteDao;

import java.io.IOException;

public class KVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new InMemoryKvService(port, new FileByteDao());
    }
}
