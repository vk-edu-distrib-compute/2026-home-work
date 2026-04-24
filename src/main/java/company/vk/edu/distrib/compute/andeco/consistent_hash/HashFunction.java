package company.vk.edu.distrib.compute.andeco.consistent_hash;

@FunctionalInterface
public interface HashFunction<K,T> {
    K hash(T value);
}
