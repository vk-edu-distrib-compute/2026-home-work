package company.vk.edu.distrib.compute.wedwincode.sharded.grpc;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStartException;
import company.vk.edu.distrib.compute.wedwincode.sharded.RendezvousKVClusterImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GrpcKVClusterImpl extends RendezvousKVClusterImpl {

    private final Map<String, Ports> endpointToMultiplePorts;

    private record Ports(int http, int grpc) {}

    public GrpcKVClusterImpl(List<Integer> httpPorts, List<Integer> grpcPorts) {
        this(buildEndpointsMap(httpPorts, grpcPorts));
    }

    private GrpcKVClusterImpl(Map<String, Ports> endpointToMultiplePorts) {
        super(new ArrayList<>(endpointToMultiplePorts.keySet()), null);
        this.endpointToMultiplePorts = endpointToMultiplePorts;
    }

    @Override
    public void start() {
        endpointToMultiplePorts.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        if (endpointToService.containsKey(endpoint)) {
            throw new ServiceStartException("service is already running on endpoint " + endpoint);
        }

        try {
            if (!endpointToMultiplePorts.containsKey(endpoint)) {
                throw new ServiceStartException("endpoint not exist: " + endpoint);
            }
            Ports ports = endpointToMultiplePorts.get(endpoint);
            KVService service = new GrpcShardedKVServiceImpl(
                    ports.http(),
                    ports.grpc(),
                    buildPersistentDao(ports.http()),
                    strategy
            );
            service.start();
            endpointToService.put(endpoint, service);
        } catch (IOException e) {
            throw new ServiceStartException("failed to start endpoint: " + endpoint, e);
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpointToMultiplePorts.keySet());
    }

    private static Map<String, Ports> buildEndpointsMap(List<Integer> httpPorts, List<Integer> grpcPorts) {
        if (httpPorts.size() != grpcPorts.size()) {
            throw new IllegalArgumentException("HTTP and gRPC lists should be the same size");
        }

        return IntStream.range(0, httpPorts.size())
                .boxed()
                .collect(Collectors.toMap(
                        i -> LOCALHOST_PREFIX + httpPorts.get(i) + "?grpcPort=" + grpcPorts.get(i),
                        i -> new Ports(httpPorts.get(i), grpcPorts.get(i)),
                        (a, b) -> b,
                        ConcurrentHashMap::new));
    }
}
