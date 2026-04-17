package company.vk.edu.distrib.compute.linempy.scharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.linempy.DaoImpl;
import company.vk.edu.distrib.compute.linempy.routing.RendezvousHashRouter;
import company.vk.edu.distrib.compute.linempy.routing.ShardingStrategy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LinempyKVClusterFactory — описание класса.
 *
 * <p>
 * TODO: добавить описание назначения и поведения класса.
 * </p>
 *
 * @author Linempy
 * @since 16.04.2026
 */
public class LinempyKVClusterFactory extends KVClusterFactory {
    //private static final String DATA_DIR = System.getProperty("persistent.data.dir", "./linempy_data");

    private static final String LOCAL_HOST = "http://localhost:";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .map(this::parseToEndpoint)
                .toList();

        ShardingStrategy router = new RendezvousHashRouter();
        Map<String, KVService> nodes = new ConcurrentHashMap<>();
        for (int port : ports) {
            String endpoint = parseToEndpoint(port);
            try {
                nodes.put(endpoint, createNode(port, endpoints, router));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new LinempyKVClusterImpl(endpoints, nodes);
    }

    private KVService createNode(int port, List<String> endpoints, ShardingStrategy router) throws IOException {
        return new KVServiceProxyImpl(port, new DaoImpl<>(), endpoints, router);
    }

    private String parseToEndpoint(Integer port) {
        return LOCAL_HOST + port;
    }
}
