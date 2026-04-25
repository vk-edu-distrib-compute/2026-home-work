package company.vk.edu.distrib.compute.arseniy90;

import java.util.List;

public enum HashStrategy {
    CONSISTENT,
    RENDEZVOUS;

    public HashRouter createRouter(List<String> endpoints) {
        return switch (this) {
            case CONSISTENT -> new ConsistentHash(endpoints);
            case RENDEZVOUS -> new RendezvousHash(endpoints);
        };
    }
}
