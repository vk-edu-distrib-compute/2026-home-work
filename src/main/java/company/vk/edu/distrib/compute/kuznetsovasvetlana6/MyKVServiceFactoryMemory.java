package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;

public class MyKVServiceFactoryMemory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new MyKVService(port, new MyDaoMemory());
    }
}
