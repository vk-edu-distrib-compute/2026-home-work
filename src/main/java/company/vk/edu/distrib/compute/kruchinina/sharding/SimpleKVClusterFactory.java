package company.vk.edu.distrib.compute.kruchinina.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

/**
 * Фабрика для создания кластера с выбором алгоритма через  свойство
 * Пример использования: -Dsharding.algorithm=consistent .
 */
public class SimpleKVClusterFactory extends KVClusterFactory {
    private static final String ALGORITHM_PROPERTY = "sharding.algorithm";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        String algoName = System.getProperty(ALGORITHM_PROPERTY, "consistent");
        KVClusterImpl.Algorithm algorithm;
        if ("rendezvous".equalsIgnoreCase(algoName)) {
            algorithm = KVClusterImpl.Algorithm.RENDEZVOUS_HASHING;
        } else {
            algorithm = KVClusterImpl.Algorithm.CONSISTENT_HASHING;
        }
        return new KVClusterImpl(ports, algorithm);
    }
}
