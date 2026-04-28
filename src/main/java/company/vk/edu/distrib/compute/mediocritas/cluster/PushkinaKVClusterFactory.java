package company.vk.edu.distrib.compute.mediocritas.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.ConsistentHashRouter;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.RendezvousHashRouter;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.Router;

import java.util.List;

public class PushkinaKVClusterFactory extends KVClusterFactory {

    private static final int VIRTUAL_NODES_COUNT = 150;
    private static final String ROUTER_TYPE_PROPERTY = "router.type";
    private static final String CONSISTENT = "consistent";
    private static final String RENDEZVOUS = "rendezvous";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Router router = createRouter();
        return new PushkinaKVCluster(ports, router);
    }

    private Router createRouter() {
        String routerType = System.getProperty(ROUTER_TYPE_PROPERTY, CONSISTENT);
        return switch (routerType) {
            case RENDEZVOUS -> new RendezvousHashRouter();
            case CONSISTENT -> new ConsistentHashRouter(VIRTUAL_NODES_COUNT);
            default -> throw new IllegalArgumentException(
                    "Unknown router type: " + routerType + ". Supported: " + CONSISTENT + ", " + RENDEZVOUS
            );
        };
    }
}
