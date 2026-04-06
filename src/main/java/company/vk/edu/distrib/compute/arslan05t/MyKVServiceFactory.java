package company.vk.edu.distrib.compute.arslan05t;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;

public class MyKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // Создаем хранилище прямо здесь
        InMemoryDao dao = new InMemoryDao();
        // Передаем его в сервис
        return new MyKVService(port, dao);
    }
}
