package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;
import java.nio.file.Path;

public class MyKVServiceFactoryFile extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // Создаем папку data в текущей директории проекта
        Path dataPath = Path.of("data"); 
        return new MyKVService(port, new MyDaoFile(dataPath));
    }
}
