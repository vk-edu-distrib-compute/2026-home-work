package company.vk.edu.distrib.compute.andeco.sharding;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface ShardingStrategy<T> {
    Node<T> get(T key);
}
