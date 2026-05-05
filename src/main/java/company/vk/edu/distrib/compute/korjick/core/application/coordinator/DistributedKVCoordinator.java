package company.vk.edu.distrib.compute.korjick.core.application.coordinator;

import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.InvalidConsistencyException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.NotEnoughReplicasException;
import company.vk.edu.distrib.compute.korjick.core.application.node.EntityNode;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class DistributedKVCoordinator implements KVCoordinator {
    private static final Logger log = LoggerFactory.getLogger(DistributedKVCoordinator.class);
    private static final int DEFAULT_ACK = 1;

    private final List<EntityNode> nodes;
    private final int replicationFactor;

    public DistributedKVCoordinator(List<EntityNode> nodes,
                                    int replicationFactor) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication factor should be positive");
        }
        if (nodes.size() < replicationFactor) {
            throw new IllegalArgumentException("Not enough nodes for replication factor");
        }

        this.nodes = List.copyOf(nodes);
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Entity get(Entity.Key key) {
        return get(key, DEFAULT_ACK);
    }

    public Entity get(Entity.Key key, int ack) {
        validateAck(ack);

        int acknowledgements = 0;
        Entity latest = null;
        for (EntityNode node : replicasFor(key)) {
            try {
                Entity entity = node.get(key);
                acknowledgements++;
                latest = newest(latest, entity);
            } catch (EntityNotFoundException e) {
                acknowledgements++;
            } catch (RuntimeException e) {
                log.warn("Failed to read endpoint={} key={}", node.endpoint(), key.value(), e);
            }
        }

        ensureEnoughAcknowledgements(ack, acknowledgements, "read", key);
        if (latest == null || latest.deleted()) {
            throw new EntityNotFoundException("No value for key: " + key.value());
        }

        return latest;
    }

    @Override
    public void upsert(Entity entity) {
        upsert(entity, DEFAULT_ACK);
    }

    public void upsert(Entity entity, int ack) {
        validateAck(ack);
        final var versioned = new Entity(
                entity.key(),
                entity.body(),
                nextVersion(),
                false
        );
        writeToReplicas(versioned, ack);
    }

    @Override
    public void delete(Entity.Key key) {
        delete(key, DEFAULT_ACK);
    }

    public void delete(Entity.Key key, int ack) {
        validateAck(ack);
        final var tombstone = new Entity(key, new byte[0], nextVersion(), true);
        writeToReplicas(tombstone, ack);
    }

    private void writeToReplicas(Entity entity, int ack) {
        var acknowledgements = 0;
        for (EntityNode node : replicasFor(entity.key())) {
            try {
                node.upsert(entity);
                acknowledgements++;
            } catch (RuntimeException e) {
                log.warn("Failed to write endpoint={} key={}", node.endpoint(), entity.key().value(), e);
            }
        }

        ensureEnoughAcknowledgements(ack, acknowledgements, "write", entity.key());
    }

    private List<EntityNode> replicasFor(Entity.Key key) {
        return preferenceList(key).stream()
                .limit(replicationFactor)
                .toList();
    }

    private List<EntityNode> preferenceList(Entity.Key key) {
        List<ScoredNode> scoredNodes = new ArrayList<>(nodes.size());
        for (EntityNode node : nodes) {
            long score = Integer.toUnsignedLong(Objects.hash(key.value(), node.endpoint()));
            scoredNodes.add(new ScoredNode(node, score));
        }
        scoredNodes.sort(Comparator.comparingLong(ScoredNode::score).reversed());

        return scoredNodes.stream()
                .map(ScoredNode::node)
                .toList();
    }

    private void validateAck(int ack) {
        if (ack < DEFAULT_ACK) {
            throw new InvalidConsistencyException("ack should be positive");
        }
        if (ack > replicationFactor) {
            throw new InvalidConsistencyException("ack cannot be greater than replication factor");
        }
    }

    private void ensureEnoughAcknowledgements(int expectedAck,
                                              int actualAck,
                                              String operation,
                                              Entity.Key key) {
        if (actualAck < expectedAck) {
            throw new NotEnoughReplicasException(String.format(
                    "Not enough replicas for %s key=%s, expected=%d, actual=%d",
                    operation,
                    key.value(),
                    expectedAck,
                    actualAck
            ));
        }
    }

    private long nextVersion() {
        return System.currentTimeMillis();
    }

    private Entity newest(Entity left, Entity right) {
        if (left == null || right.version() > left.version()) {
            return right;
        }
        return left;
    }

    private record ScoredNode(EntityNode node, long score) {
    }
}
