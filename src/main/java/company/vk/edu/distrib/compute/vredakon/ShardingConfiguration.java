package company.vk.edu.distrib.compute.vredakon;

public class ShardingConfiguration {
    // private static final String[] STRATEGIES = {"rendezvous", "consistent"};
    private final String strategy;

    public ShardingConfiguration() {
        strategy = "consistent";
    }

    // private void setRendezvous() {
    //     strategy = "rendezvous";
    // }

    // private void setConsistent() {
    //     strategy = "consistent";
    // }

    public String strategy() {
        return strategy;
    }
}
