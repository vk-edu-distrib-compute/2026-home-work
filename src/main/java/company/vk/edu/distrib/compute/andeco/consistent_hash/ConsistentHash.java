package company.vk.edu.distrib.compute.andeco.consistent_hash;

import company.vk.edu.distrib.compute.andeco.sharding.Node;
import company.vk.edu.distrib.compute.andeco.sharding.ShardingStrategy;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash<N extends Node, K> implements ShardingStrategy<K> {

  private final HashFunction<Long, String> hashFunction;
  private final int numberOfReplicas;
  private final SortedMap<Long, N> circle = new TreeMap<>();

  public ConsistentHash(HashFunction<Long, String> hashFunction,
                        int numberOfReplicas,
                        Collection<N> nodes) {
    this.hashFunction = hashFunction;
    this.numberOfReplicas = numberOfReplicas;

    for (N node : nodes) {
      add(node);
    }
  }

  public void add(N node) {
    for (int i = 0; i < numberOfReplicas; i++) {
      long hash = hashFunction.hash(node.getId() + i);
      circle.put(hash, node);
    }
  }

  @Override
  public N get(K key) {
    if (circle.isEmpty()) {
      return null;
    }

    long hash = hashFunction.hash(String.valueOf(key));

    if (!circle.containsKey(hash)) {
      SortedMap<Long, N> tailMap = circle.tailMap(hash);
      hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }

    return circle.get(hash);
  }
}
