package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.martinez1337.dao.FileDao;

import java.io.IOException;
import java.nio.file.Path;

public class DefaultKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new DefaultKVService(port, new FileDao(Path.of(".kv_data")));
    }
}
