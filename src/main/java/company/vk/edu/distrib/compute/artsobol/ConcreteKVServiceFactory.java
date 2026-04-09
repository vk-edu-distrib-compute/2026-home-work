package company.vk.edu.distrib.compute.artsobol;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.artsobol.dao.InMemoryDao;

import java.io.IOException;

public class ConcreteKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, new InMemoryDao());
    }
}
