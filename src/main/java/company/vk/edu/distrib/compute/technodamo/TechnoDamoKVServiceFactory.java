package company.vk.edu.distrib.compute.technodamo;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TechnoDamoKVServiceFactory extends KVServiceFactory {
    private static final Path ROOT = Path.of(".data", "technodamo");

    @Override
    protected KVService doCreate(int port) throws IOException {
        Files.createDirectories(ROOT);
        Path storagePath = ROOT.resolve(Integer.toString(port));
        Dao<byte[]> dao = new FileDao(storagePath);
        return new TechnoDamoKVService(port, dao);
    }
}
