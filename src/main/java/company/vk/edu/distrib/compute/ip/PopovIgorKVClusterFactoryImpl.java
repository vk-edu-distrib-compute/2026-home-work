package company.vk.edu.distrib.compute.ip;

import java.util.List;
import java.util.stream.Collectors;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

public class PopovIgorKVClusterFactoryImpl extends KVClusterFactory {
    
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .map(port -> "http://[::]:" + port)
                .collect(Collectors.toList());

        return new PopovIgorKVClusterImpl(endpoints);
    }
}
