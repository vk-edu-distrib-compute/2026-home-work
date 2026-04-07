package company.vk.edu.distrib.compute.yyaarr;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class YyaarrKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new YyaarrKVService(port, new YyaarrInMemoryDao());
    }
}
