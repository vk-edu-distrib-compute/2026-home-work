package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.KVService;

import java.util.Map;

public final class Strategies {
    private Strategies() {
        throw new UnsupportedOperationException();
    }

    public static String resolve(String key, Map<String, KVService> nodes) {
        String server = "";
        int maxHash = Integer.MIN_VALUE;
        for (String node: nodes.keySet()) {
            int hash = (key.hashCode() + "#" + node).hashCode();
            if (hash > maxHash) {
                maxHash = hash;
                server = node;
            }
        }
        return server;
    }
}
