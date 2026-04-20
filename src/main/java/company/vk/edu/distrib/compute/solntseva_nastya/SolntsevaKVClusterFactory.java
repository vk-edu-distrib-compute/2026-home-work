package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService; // Исправлен импорт
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates a sharded KV cluster using rendezvous hashing.
 */
public class SolntsevaKVClusterFactory extends KVClusterFactory {

    @Override
    public KVCluster doCreate(List<Integer> ports) {
        try {
            List<String> endpoints = ports.stream()
                    .map(port -> "http://localhost:" + port)
                    .toList();

            Set<String> topology = new HashSet<>(endpoints);

            List<KVService> services = new ArrayList<>(); // Исправлена типизация
            final String storageBase = "storage"; // Оптимизация для Codacy

            for (int port : ports) {
                String myUrl = "http://localhost:" + port;
                
                // Теперь мы не создаем каждый раз новый объект Path для базовой директории
                Path daoPath = Paths.get(storageBase, "data_" + port);
                PersistentDao dao = new PersistentDao(daoPath);
                
                services.add(new SolntsevaKVService(
                        port,
                        dao,
                        topology,
                        myUrl,
                        SolnHashiStrategy.RENDEZVOUS
                ));
            }
            return new SolntsevaKVCluster(services, endpoints);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create cluster nodes", ex);
        }
    }
}
