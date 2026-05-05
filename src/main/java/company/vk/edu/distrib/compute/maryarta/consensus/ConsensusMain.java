package company.vk.edu.distrib.compute.maryarta.consensus;

import java.util.List;

public class ConsensusMain {
    void main(String... args) {
        Cluster cluster = new Cluster(List.of(42, 75, 82, 13));
        cluster.startNodes();
    }
}
