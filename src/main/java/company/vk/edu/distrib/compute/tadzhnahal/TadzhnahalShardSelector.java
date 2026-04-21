package company.vk.edu.distrib.compute.tadzhnahal;

@FunctionalInterface
public interface TadzhnahalShardSelector {
    String select(String key);
}
