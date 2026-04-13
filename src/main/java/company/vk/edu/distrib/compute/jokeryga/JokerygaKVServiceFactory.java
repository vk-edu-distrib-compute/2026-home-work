package company.vk.edu.distrib.compute.jokeryga;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class JokerygaKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new JokerygaKVService(port, new InMemoryDao());
    }
}
