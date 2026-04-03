package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class TadzhnahalKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        InMemoryDao dao = new InMemoryDao();
        return new TadzhnahalKVService(port, dao);
    }
}
