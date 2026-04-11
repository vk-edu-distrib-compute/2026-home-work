package company.vk.edu.distrib.compute.lillymega;
//реализация класса и фабрики
import  company.vk.edu.distrib.compute.KVService;
import  company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class LillymegaKVServiceFactory extends KVServiceFactory{
    @Override
    protected KVService doCreate(int port) throws IOException{
        return new LillymegaKVService(port, new Dao<byte[]> dao);
    }
}
