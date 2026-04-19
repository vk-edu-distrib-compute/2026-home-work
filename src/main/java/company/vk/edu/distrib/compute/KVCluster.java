package company.vk.edu.distrib.compute.goshanchic;

import java.io.IOException;

public interface KVCluster {

    void start() throws IOException;

    void stop();
}
