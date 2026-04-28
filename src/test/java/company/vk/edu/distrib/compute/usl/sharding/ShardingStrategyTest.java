package company.vk.edu.distrib.compute.usl.sharding;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingStrategyTest {
    private static final List<String> ENDPOINTS = List.of(
        "http://localhost:18080",
        "http://localhost:18081",
        "http://localhost:18082"
    );

    @Test
    void rendezvousIsDeterministic() {
        ShardingStrategy strategy = new RendezvousShardingStrategy(ENDPOINTS);

        String firstOwner = strategy.resolveOwner("alpha");
        for (int i = 0; i < 50; i++) {
            assertEquals(firstOwner, strategy.resolveOwner("alpha"));
        }
        assertTrue(ENDPOINTS.contains(firstOwner));
    }

    @Test
    void consistentHashingIsDeterministic() {
        ShardingStrategy strategy = new ConsistentShardingStrategy(ENDPOINTS);

        String firstOwner = strategy.resolveOwner("beta");
        for (int i = 0; i < 50; i++) {
            assertEquals(firstOwner, strategy.resolveOwner("beta"));
        }
        assertTrue(ENDPOINTS.contains(firstOwner));
    }

    @Test
    void algorithmParserSupportsBothVariants() {
        assertEquals(ShardingAlgorithm.RENDEZVOUS, ShardingAlgorithm.fromExternalName("rendezvous"));
        assertEquals(ShardingAlgorithm.CONSISTENT, ShardingAlgorithm.fromExternalName("consistent"));
    }
}
