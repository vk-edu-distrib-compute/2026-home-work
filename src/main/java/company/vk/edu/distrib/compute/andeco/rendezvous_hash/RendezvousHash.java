package company.vk.edu.distrib.compute.andeco.rendezvous_hash;

import company.vk.edu.distrib.compute.andeco.consistent_hash.HashFunction;
import company.vk.edu.distrib.compute.andeco.sharding.Node;
import company.vk.edu.distrib.compute.andeco.sharding.ShardingStrategy;

import java.util.Collection;

public class RendezvousHash<N extends Node> implements ShardingStrategy<String> {

    private final HashFunction<Long, String> hashFunction;
    private final Collection<N> nodes;

    public RendezvousHash(HashFunction<Long, String> hashFunction,
                          Collection<N> nodes) {
        this.hashFunction = hashFunction;
        this.nodes = nodes;
    }

    @Override
    public N get(String key) {
        N bestNode = null;
        long bestScore = Long.MIN_VALUE;

        for (N node : nodes) {
            long score = computeScore(key, node);

            if (score > bestScore) {
                bestScore = score;
                bestNode = node;
            }
        }

        return bestNode;
    }

    private long computeScore(String key, N node) {
        return hashFunction.hash(key + ":" + node.getId());
    }
}
