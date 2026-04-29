package company.vk.edu.distrib.compute.korjick.core.application;

import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.InvalidConsistencyException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.NotEnoughReplicasException;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class ReplicatedKVCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVCoordinator.class);

    private final List<String> replicaEndpoints;
    private final EntityGateway gateway;
    private final Predicate<String> endpointAvailable;
    private final int replicationFactor;
    private final AtomicLong versionGenerator;

    public ReplicatedKVCoordinator(List<String> replicaEndpoints,
                                   EntityGateway gateway,
                                   Predicate<String> endpointAvailable,
                                   int replicationFactor) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication factor should be positive");
        }
        if (replicaEndpoints.size() < replicationFactor) {
            throw new IllegalArgumentException("Not enough replica endpoints");
        }

        this.replicaEndpoints = List.copyOf(replicaEndpoints);
        this.gateway = gateway;
        this.endpointAvailable = endpointAvailable;
        this.replicationFactor = replicationFactor;
        this.versionGenerator = new AtomicLong(0);
    }

    public Entity get(Entity.Key key, int ack) {
        validateAck(ack);

        int acknowledgements = 0;
        Entity latest = null;
        for (String endpoint : replicaEndpoints(key)) {
            if (!endpointAvailable.test(endpoint)) {
                continue;
            }

            try {
                Entity entity = gateway.getEntity(endpoint, key);
                acknowledgements++;
                latest = newest(latest, entity);
            } catch (EntityNotFoundException e) {
                acknowledgements++;
            } catch (RuntimeException e) {
                log.warn("Failed to read endpoint={} key={}", endpoint, key.value(), e);
            }
        }

        ensureEnoughAcknowledgements(ack, acknowledgements, "read", key);
        if (latest == null || latest.deleted()) {
            throw new EntityNotFoundException("No value for key: " + key.value());
        }

        return latest;
    }

    public void upsert(Entity entity, int ack) {
        validateAck(ack);

        Entity replicatedEntity = new Entity(
                entity.key(),
                entity.body(),
                nextVersion(),
                false
        );
        writeToReplicas(replicatedEntity, ack);
    }

    public void delete(Entity.Key key, int ack) {
        validateAck(ack);

        Entity tombstone = new Entity(key, new byte[0], nextVersion(), true);
        writeToReplicas(tombstone, ack);
    }

    private void writeToReplicas(Entity entity, int ack) {
        int acknowledgements = 0;
        for (String endpoint : replicaEndpoints(entity.key())) {
            if (!endpointAvailable.test(endpoint)) {
                continue;
            }

            try {
                gateway.upsertEntity(endpoint, entity);
                acknowledgements++;
            } catch (RuntimeException e) {
                log.warn("Failed to write endpoint={} key={}", endpoint, entity.key().value(), e);
            }
        }

        ensureEnoughAcknowledgements(ack, acknowledgements, "write", entity.key());
    }

    private List<String> replicaEndpoints(Entity.Key key) {
        List<ScoredEndpoint> endpoints = new ArrayList<>(replicaEndpoints.size());
        for (String endpoint : replicaEndpoints) {
            long score = Integer.toUnsignedLong(Objects.hash(key.value(), endpoint));
            endpoints.add(new ScoredEndpoint(endpoint, score));
        }
        endpoints.sort(Comparator.comparingLong(ScoredEndpoint::score).reversed());

        return endpoints.stream()
                .limit(replicationFactor)
                .map(ScoredEndpoint::endpoint)
                .toList();
    }

    private void validateAck(int ack) {
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
        return versionGenerator.addAndGet(1);
    }

    private Entity newest(Entity left, Entity right) {
        if (left == null || right.version() > left.version()) {
            return right;
        }
        return left;
    }

    private record ScoredEndpoint(String endpoint, long score) {
    }
}
