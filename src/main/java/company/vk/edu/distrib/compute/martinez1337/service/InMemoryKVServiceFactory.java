package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.martinez1337.dao.InMemoryDao;

import java.io.IOException;

public class InMemoryKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new InMemoryKVService(port, new InMemoryDao());
    }
}
