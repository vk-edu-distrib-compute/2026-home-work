package company.vk.edu.distrib.compute;

import java.util.List;

public interface KVCluster {
    void start();

    void start(String endpoint);

    void stop();

    void stop(String endpoint);

    List<String> getEndpoints();
}
