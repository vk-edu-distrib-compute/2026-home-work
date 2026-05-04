package company.vk.edu.distrib.compute.ce_fello;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CeFelloKeyDistributorTest {
    private static final String STABLE_KEY = "stable-key";
    private static final List<String> ENDPOINTS = List.of(
            "http://localhost:8080",
            "http://localhost:8081",
            "http://localhost:8082"
    );

    @Test
    void rendezvousDistributorIsStable() {
        CeFelloKeyDistributor distributor = new CeFelloRendezvousKeyDistributor(ENDPOINTS);

        String owner = distributor.ownerFor(STABLE_KEY);

        for (int i = 0; i < 10; i++) {
            assertEquals(owner, distributor.ownerFor(STABLE_KEY));
        }
        assertTrue(ENDPOINTS.contains(owner));
    }

    @Test
    void consistentHashDistributorIsStable() {
        CeFelloKeyDistributor distributor = new CeFelloConsistentHashKeyDistributor(ENDPOINTS);

        String owner = distributor.ownerFor(STABLE_KEY);

        for (int i = 0; i < 10; i++) {
            assertEquals(owner, distributor.ownerFor(STABLE_KEY));
        }
        assertTrue(ENDPOINTS.contains(owner));
    }
}
