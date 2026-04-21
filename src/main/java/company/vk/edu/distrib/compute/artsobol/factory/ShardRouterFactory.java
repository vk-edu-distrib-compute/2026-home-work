package company.vk.edu.distrib.compute.artsobol.factory;

import company.vk.edu.distrib.compute.artsobol.router.ConsistentHashShardRouter;
import company.vk.edu.distrib.compute.artsobol.router.RendezvousShardRouter;
import company.vk.edu.distrib.compute.artsobol.router.ShardRouter;

import java.util.List;
import java.util.Locale;

public final class ShardRouterFactory {
    static final String ALGORITHM_PROPERTY = "sharding.algorithm";

    private ShardRouterFactory() {
    }

    public static ShardRouter create(List<String> endpoints) {
        String rawAlgorithm = System.getProperty(ALGORITHM_PROPERTY, "rendezvous");
        String algorithm = rawAlgorithm.toLowerCase(Locale.ROOT);

        return switch (algorithm) {
            case "consistent" -> new ConsistentHashShardRouter(endpoints);
            case "rendezvous" -> new RendezvousShardRouter(endpoints);
            default -> throw new IllegalArgumentException("Unknown sharding algorithm: " + rawAlgorithm);
        };
    }
}
