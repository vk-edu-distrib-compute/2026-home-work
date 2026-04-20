package company.vk.edu.distrib.compute;

import java.io.IOException;
import java.util.List;

public interface KVCluster {
    // стартует все ноды кластера
    void start() throws IOException;

    // стартует одну определенную ноду кластера
    void start(String endpoint) throws IOException;

    // останавливает все ноды кластера
    void stop();

    // останавливает одну определенную ноду кластера
    void stop(String endpoint);

    // отдаёт эндпойнты нод кластера
    List<String> getEndpoints();
}
