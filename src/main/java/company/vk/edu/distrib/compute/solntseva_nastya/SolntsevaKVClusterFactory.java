package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService; 
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SolntsevaKVClusterFactory extends KVClusterFactory {
    @Override
    public KVCluster doCreate(List<Integer> ports) {
        try {
            List<String> endpoints = ports.stream()
                    .map(port -> "http://localhost:" + port)
                    .toList();

            Set<String> topology = endpoints.stream().collect(Collectors.toSet());

            List<KVService> services = new ArrayList<>();
            for (int port : ports) {
                String myUrl = "http://localhost:" + port;
                java.nio.file.Path storagePath = java.nio.file.Paths.get("storage", "data_" + port);
                PersistentDao dao = new PersistentDao(storagePath);
                
                services.add(new SolntsevaKVService(
                        port, 
                        dao, 
                        topology, 
                        myUrl, 
                        SolnHashiStrategy.RENDEZVOUS
                ));
            }
            return new SolntsevaKVCluster(services, endpoints);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create cluster nodes", e);
        }
    }
}
