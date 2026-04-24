package company.vk.edu.distrib.compute.dariaprindina;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

interface DPShardSelector {
    String ownerForKey(String key);

    List<String> replicasForKey(String key, int count);

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

        @Override
        public List<String> replicasForKey(String key, int count) {
            if (count < 1 || count > endpoints.size()) {
                throw new IllegalArgumentException("bad replicas count");
            }
            final int startIndex = Math.floorMod(key.hashCode(), endpoints.size());
            return java.util.stream.IntStream.range(0, count)
                .mapToObj(offset -> endpoints.get((startIndex + offset) % endpoints.size()))
                .toList();
        }
    }

    final class RendezvousShardSelector implements DPShardSelector {
        private final List<String> endpoints;

        private RendezvousShardSelector(List<String> endpoints) {
            this.endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
        }

        @Override
        public String ownerForKey(String key) {
            return replicasForKey(key, 1).getFirst();
        }

        @Override
        public List<String> replicasForKey(String key, int count) {
            if (count < 1 || count > endpoints.size()) {
                throw new IllegalArgumentException("bad replicas count");
            }
            return endpoints.stream()
                .sorted((left, right) -> Long.compare(score(key, right), score(key, left)))
                .limit(count)
                .collect(Collectors.toList());
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
