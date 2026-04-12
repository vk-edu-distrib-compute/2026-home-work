package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SolntsevaKVClusterFactory extends KVClusterFactory {
    @Override
    public KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .toList();

        Set<String> topology = endpoints.stream().collect(Collectors.toSet());
        List<KVService> services = new ArrayList<>(ports.size());

        try {
            for (int port : ports) {
                String myUrl = "http://localhost:" + port;
                Path storagePath = Paths.get("storage", "data_" + port);
                
                services.add(new SolntsevaKVService(
                        port,
                        new PersistentDao(storagePath),
                        topology,
                        myUrl
                ));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize cluster storage nodes", e);
        }

        return new SolntsevaKVCluster(services, endpoints);
    }
}
