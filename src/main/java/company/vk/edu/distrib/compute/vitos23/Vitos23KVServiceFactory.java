package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class Vitos23KVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // Dao<byte[]> dao = new InMemoryDao<>();
        Dao<byte[]> dao = new WalBackedDao("storage/vitos23/single");
        EntityRequestProcessor entityRequestProcessor = new DirectEntityRequestProcessor(dao);
        return new KVServiceImpl(port, entityRequestProcessor);
    }
}
