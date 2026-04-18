package company.vk.edu.distrib.compute.nst1610.sharding;

import java.util.List;

public interface HashingStrategy {
    String resolve(String key);

    void updateEndpoints(List<String> endpoints);
}
