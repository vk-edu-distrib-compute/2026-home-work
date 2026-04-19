package company.vk.edu.distrib.compute.nihuaway00.sharding;

import java.util.concurrent.atomic.AtomicBoolean;

public class NodeInfo {
    private final String endpoint;
    private final AtomicBoolean alive = new AtomicBoolean(false);

    public NodeInfo(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isEnabled() {
        return alive.get();
    }

    public void enable() {
        alive.set(true);
    }

    public void disable() {
        alive.set(false);
    }
}
