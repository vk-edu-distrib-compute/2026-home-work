package company.vk.edu.distrib.compute.denchika.factory;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.denchika.dao.InMemoryDao;
import company.vk.edu.distrib.compute.denchika.service.SimpleKVService;

import java.io.IOException;

public class DenchikaInMemoryKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new SimpleKVService(port, new InMemoryDao());
    }
}
