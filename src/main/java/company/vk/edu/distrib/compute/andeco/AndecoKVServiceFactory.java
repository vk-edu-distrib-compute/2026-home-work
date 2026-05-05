package company.vk.edu.distrib.compute.andeco;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.andeco.controller.EntityController;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;

import java.io.IOException;
import java.nio.file.Path;

public class AndecoKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path data = Path.of("..", "data");
        KVServiceImpl service = new KVServiceImpl(port,
                new EntityController(new FileDao(data)),
                new StatusController());
        service.registerDefault();
        return service;
    }
}
