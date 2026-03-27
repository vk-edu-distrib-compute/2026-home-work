package company.vk.edu.distrib.compute.vodobryshkin;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class DefaultKVServiceFactory extends KVServiceFactory {
    private static final int BACKLOG_SIZE = 128;

    @Override
    public KVService doCreate(int port) throws IOException {
        return new DefaultKVService(port, BACKLOG_SIZE);
    }
}
