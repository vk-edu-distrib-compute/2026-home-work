package company.vk.edu.distrib.compute.tadzhnahal.consensus;

public final class App {
    private static final int DEFAULT_NODE_COUNT = 5;

    private App() {
    }

    public static void main(String[] args) {
        Cluster cluster = new Cluster(DEFAULT_NODE_COUNT);
        cluster.printState();
    }
}
