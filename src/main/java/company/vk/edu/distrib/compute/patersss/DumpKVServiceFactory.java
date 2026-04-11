package company.vk.edu.distrib.compute.patersss;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class DumpKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new DumpKVService(port, new DumpDAO());
    }
}
