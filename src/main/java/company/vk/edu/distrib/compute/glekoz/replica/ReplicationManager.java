package company.vk.edu.distrib.compute.glekoz.replica;

import company.vk.edu.distrib.compute.glekoz.KVServiceGK;
import company.vk.edu.distrib.compute.glekoz.replica.records.HostNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class ReplicationManager {
    private final Map<Integer, HostNode> nodesByID = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final String LOCALHOST = "http://localhost:";
    private final int replicationFactor;
    private static final int MIN_REPLICAS = 3;
    private static final int MAX_PORT_ATTEMPTS = 100;

    public ReplicationManager(int port) {
        this.replicationFactor = setReplicationFactor();
        for (int nodeId = 0; nodeId < replicationFactor; nodeId++) {
            HostNode hostNode = startService(port, nodeId);
            nodesByID.put(nodeId, hostNode);
        }
    }

    public List<String> getReplicaHostList() {
        List<String> hosts = new ArrayList<>();
        for (HostNode hn : nodesByID.values()) {
            hosts.add(hn.host());
        }
        return hosts;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public void enableReplica(int id) {
        HostNode hn = nodesByID.get(id);
        if (hn == null) {
            log.warn("Attempted to enable non-existent replica with ID: {}", id);
            return;
        }
        if (hn.node() != null) {
            log.warn("Attempted to start existing replica with ID: {}", id);
            return;
        }
        try {
            nodesByID.put(id, startServiceWithHost(hn.host()));
        } catch (Exception e) {
            log.error("Failed to enable replica with ID: {}", id, e);
            throw new IllegalStateException("Failed to enable replica: " + id, e);
        }
    }

    public void disableReplica(int id) {
        HostNode hn = nodesByID.get(id);
        if (hn == null) {
            log.warn("Attempted to enable non-existent replica with ID: {}", id);
            return;
        }
        KVServiceGK service = hn.node();
        if (service == null) {
            log.warn("Attempted to stop stopped replica with ID: {}", id);
            return;
        }
        try {
            service.stop();
            nodesByID.put(id, new HostNode(hn.host(), null));
        } catch (Exception e) {
            log.error("Failed to disable replica with ID: {}", id, e);
            throw new IllegalStateException("Failed to disable replica: " + id, e);
        }
    }

    private HostNode startServiceWithHost(String host) {
        int nodePort = toPort(host);
        try {
            KVServiceGK service = new KVServiceGK(nodePort);
            service.start();
            return new HostNode(host, service);
        } catch (Exception e) {
                log.warn("Failed to start service on host {}", 
                    host, e);
        }
        throw new IllegalStateException("Unable to start service for node on " + host);
    }

    private int setReplicationFactor() {
        String value = System.getenv("REPLICATION_FACTOR");

        if (value == null || value.isBlank()) {
            return 5;
        }

        try {
            int res = Integer.parseInt(value);
            if (res < MIN_REPLICAS) {
                throw new IllegalArgumentException("At least 3 replicas expected");
            }
            return res;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replication factor env value: " + value, e);
        }
    }

    private HostNode startService(int port, int nodeId) {
        int portAttempt = 0;
        
        while (portAttempt < MAX_PORT_ATTEMPTS) {
            int nodePort = port + 1 + nodeId + portAttempt;
            
            try {
                KVServiceGK service = serviceWrapper(nodePort);
                service.start();
                
                String endpoint = toEndpoint(nodePort);
                return new HostNode(endpoint, service);
                
            } catch (Exception e) {
                log.warn("Failed to start service on port {} for node ID {}: {}", 
                        port, nodeId, e);
                portAttempt++;
                if (portAttempt >= MAX_PORT_ATTEMPTS) {
                    throw new IllegalStateException(
                        String.format("Failed to start service for node ID %d after %d attempts", 
                            nodeId, MAX_PORT_ATTEMPTS), e);
                }
            }
        }
        throw new IllegalStateException("Unable to start service for node ID: " + nodeId);
    }

    private KVServiceGK serviceWrapper(int port) {
        return new KVServiceGK(port);
    }

    private String toEndpoint(int port) {
        return LOCALHOST + port;
    }

    private int toPort(String endpoint) {
        return Integer.parseInt(endpoint.substring(LOCALHOST.length()));
    }
}
