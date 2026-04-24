package company.vk.edu.distrib.compute.borodinavalera1996dev.hashing;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import company.vk.edu.distrib.compute.borodinavalera1996dev.cluster.Node;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RendezvousStrategy implements HashingStrategy {

    private final HashFunction hf = Hashing.murmur3_128();
    private final Collection<Node> nodes;

    public RendezvousStrategy(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    public List<Node> getNodes(String key, int numberOfReplicated) {
        List<WeightNode> weightNodes = new ArrayList<>();
        for (Node node : nodes) {
            weightNodes.add(new WeightNode(node, hash(key + node)));
        }
        weightNodes.sort(Comparator.comparingLong(WeightNode::weight).reversed());

        List<Node> nodes = new ArrayList<>(weightNodes.size());
        for (int i = 0; i < numberOfReplicated; i++) {
            if (weightNodes.get(i).node.status()) {
                nodes.add(weightNodes.get(i).node);
            }
        }
        return nodes;
    }

    @Override
    public Node getNode(String key) {
        return getNodes(key, 1).getFirst();
    }

    private long hash(String key) {
        HashCode hc = hf.hashString(key, StandardCharsets.UTF_8);
        return hc.asLong();
    }

    private record WeightNode(Node node, long weight) {
    }
}
