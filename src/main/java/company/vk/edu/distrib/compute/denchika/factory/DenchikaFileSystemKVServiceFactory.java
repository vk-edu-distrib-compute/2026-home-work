package company.vk.edu.distrib.compute.denchika.factory;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.denchika.dao.FileSystemDao;
import company.vk.edu.distrib.compute.denchika.service.InMemoryKVService;

import java.io.IOException;
import java.nio.file.Path;

public class DenchikaFileSystemKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new InMemoryKVService(port, new FileSystemDao(Path.of("denchika-storage")));
    }
}
