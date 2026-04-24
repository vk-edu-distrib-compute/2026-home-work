package company.vk.edu.distrib.compute.vladislavguzov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public record ClusterPorts(String portsNumber) {
    private static final Logger log = LoggerFactory.getLogger(ClusterPorts.class);
    private static final int STARTING_CLUSTER_PORT = 8080;

    public List<Integer> getPortsList() {
        int counter = 0;
        try {
            counter = Integer.parseInt(portsNumber);
        } catch (NumberFormatException e) {
            log.error("Incorrect cluster's nodes count argument");
        }
        List<Integer> portsList = new ArrayList<>();
        for (int i = 0; i < counter; i++) {
            portsList.add(STARTING_CLUSTER_PORT + i);
        }
        return portsList;
    }
}
