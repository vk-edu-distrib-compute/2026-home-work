package company.vk.edu.distrib.compute.borodinavalera1996dev.cluster;

public final class Node {
    private final String name;
    boolean status;

    public Node(String name, boolean status) {
        this.name = name;
        this.status = status;
    }

    public String name() {
        return name;
    }

    public boolean status() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }
}
