package company.vk.edu.distrib.compute.nihuaway00.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterNode {
    private final String endpoint;
    private final String grpcEndpoint;
    private final AtomicBoolean alive = new AtomicBoolean(false);

    public ClusterNode(String endpoint, String grpcEndpoint) {
        this.endpoint = endpoint;
        this.grpcEndpoint = grpcEndpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getGrpcEndpoint() {
        return grpcEndpoint;
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
