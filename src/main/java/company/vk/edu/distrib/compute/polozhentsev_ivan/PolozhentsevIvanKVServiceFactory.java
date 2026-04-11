package company.vk.edu.distrib.compute.polozhentsev_ivan;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public final class PolozhentsevIvanKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new PolozhentsevIvanKVService(port, new InMemoryDao());
    }
}
