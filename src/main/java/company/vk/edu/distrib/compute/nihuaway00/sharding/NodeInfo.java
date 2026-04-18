package company.vk.edu.distrib.compute.nihuaway00.sharding;

public class NodeInfo {
    private final String endpoint;
    private final int port;
    private boolean alive;

    public NodeInfo(String endpoint, int port) {
        this.endpoint = endpoint;
        this.port = port;
    }


    public int getPort() {
        return port;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }
}
