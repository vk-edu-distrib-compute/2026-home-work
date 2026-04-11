package company.vk.edu.distrib.compute.mariguss;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;

public class MarigussKVServiceFactory extends KVServiceFactory {
    
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new MarigussKVService(port);
    }
}