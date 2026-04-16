package company.vk.edu.distrib.compute.teeaamma;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class TeeaammaKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new TeeaammaKVService(port, new TeeaammaDao());
    }
}
