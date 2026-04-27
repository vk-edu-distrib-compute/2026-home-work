package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.RendezvousHashRouter;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Создаёт кластер с Rendezvous Hashing (HRW) и gRPC внутренним транспортом.
 *
 * <p>Каждый узел получает два порта: httpPort (из переданного списка) и grpcPort (выбирается автоматически).
 */
public class ShuuuurikGrpcRendezvousKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> httpPorts) {
        // Для каждого HTTP-порта выбираем свободный gRPC-порт
        List<NodePorts> nodePorts = allocateGrpcPorts(httpPorts);

        // HTTP-эндпоинты (то что видят внешние клиенты и что используется роутером)
        List<String> allHttpEndpoints = toHttpEndpoints(httpPorts);

        // HTTP-endpoint -> grpcPort (нужна для построения gRPC-клиентов)
        Map<String, Integer> grpcPortMap = buildGrpcPortMap(nodePorts);

        RendezvousHashRouter router = new RendezvousHashRouter();

        Map<String, KVService> nodes = new ConcurrentHashMap<>();
        for (NodePorts ports : nodePorts) {
            String httpEndpoint = "http://localhost:" + ports.httpPort();
            nodes.put(httpEndpoint, createNode(ports, allHttpEndpoints, grpcPortMap, router));
        }

        return new ShuuuurikKVCluster(nodes, allHttpEndpoints);
    }

    /**
     * Создаёт один узел кластера с собственным InMemoryDao и gRPC-транспортом.
     *
     * @param ports            пара портов (httpPort, grpcPort) данного узла
     * @param allHttpEndpoints все HTTP-эндпоинты кластера
     * @param grpcPortMap      карта HTTP-endpoint -> grpcPort
     * @param router           алгоритм маршрутизации
     * @return готовый {@link KVService} для данного узла
     */
    private KVService createNode(
            NodePorts ports,
            List<String> allHttpEndpoints,
            Map<String, Integer> grpcPortMap,
            RendezvousHashRouter router
    ) {
        return new KVServiceGrpcProxyImpl(
                ports.httpPort(),
                ports.grpcPort(),
                new InMemoryDao(),
                allHttpEndpoints,
                grpcPortMap,
                router
        );
    }

    /**
     * Для каждого HTTP-порта находит свободный gRPC-порт.
     *
     * @param httpPorts список HTTP-портов
     * @return список пар (httpPort, grpcPort)
     */
    private List<NodePorts> allocateGrpcPorts(List<Integer> httpPorts) {
        List<NodePorts> result = new ArrayList<>();
        for (int httpPort : httpPorts) {
            int grpcPort = findFreePort();
            result.add(new NodePorts(httpPort, grpcPort));
        }
        return result;
    }

    /**
     * Находит свободный TCP-порт.
     */
    private int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot find a free port", e);
        }
    }

    private List<String> toHttpEndpoints(List<Integer> httpPorts) {
        List<String> endpoints = new ArrayList<>(httpPorts.size());
        for (int port : httpPorts) {
            endpoints.add("http://localhost:" + port);
        }
        return endpoints;
    }

    /**
     * Строит карту HTTP-endpoint -> grpcPort для передачи в каждый узел.
     */
    private Map<String, Integer> buildGrpcPortMap(List<NodePorts> nodePorts) {
        Map<String, Integer> map = new ConcurrentHashMap<>();
        for (NodePorts ports : nodePorts) {
            map.put("http://localhost:" + ports.httpPort(), ports.grpcPort());
        }
        return map;
    }
}
