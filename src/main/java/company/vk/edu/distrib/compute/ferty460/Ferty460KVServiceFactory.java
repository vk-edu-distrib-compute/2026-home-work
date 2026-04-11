package company.vk.edu.distrib.compute.ferty460;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class Ferty460KVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new InMemoryDao();
        return new Ferty460KVService(port, dao);
    }

}
