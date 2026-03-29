package company.vk.edu.distrib.compute.ronshinvsevolod;

import java.io.IOException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class InMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new InMemoryDao();
        return new MyKVService(dao, port);
    }
}
