package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.util.List;

public class KVServiceFactoryImpl extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        InMemoryDao dao = new InMemoryDao();
        return new KVServiceImpl(port, List.of(port), dao);
    }
}

