package company.vk.edu.distrib.compute.dariaprindina;

import java.util.List;
import java.util.Objects;

@FunctionalInterface
interface DPShardSelector {
    String ownerForKey(String key);

    static DPShardSelector modulo(List<String> endpoints) {
        return new ModuloShardSelector(endpoints);
    }

    static DPShardSelector rendezvous(List<String> endpoints) {
        return new RendezvousShardSelector(endpoints);
    }

    final class ModuloShardSelector implements DPShardSelector {
        private final List<String> endpoints;

        private ModuloShardSelector(List<String> endpoints) {
            this.endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
        }

        @Override
        public String ownerForKey(String key) {
            final int index = Math.floorMod(key.hashCode(), endpoints.size());
            return endpoints.get(index);
        }
    }

    final class RendezvousShardSelector implements DPShardSelector {
        private final List<String> endpoints;

        private RendezvousShardSelector(List<String> endpoints) {
            this.endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
        }

        @Override
        public String ownerForKey(String key) {
            String bestEndpoint = endpoints.getFirst();
            long bestScore = Long.MIN_VALUE;
            for (String endpoint : endpoints) {
                final long score = score(key, endpoint);
                if (score > bestScore) {
                    bestScore = score;
                    bestEndpoint = endpoint;
                }
            }
            return bestEndpoint;
        }

        private static long score(String key, String endpoint) {
            long hash = 1469598103934665603L;
            final String value = key + "|" + endpoint;
            for (int i = 0; i < value.length(); i++) {
                hash ^= value.charAt(i);
                hash *= 1099511628211L;
            }
            return hash;
        }
    }
}
