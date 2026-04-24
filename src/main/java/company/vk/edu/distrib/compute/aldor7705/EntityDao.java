package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.aldor7705.storage.DaoFileStorage;

import java.nio.file.Path;
import java.util.*;

public class EntityDao implements Dao<byte[]> {

    private final List<DaoFileStorage> storages;
    private final boolean[] disabledReplicas;

    public EntityDao(Path path, int replicas) {
        this.storages = new ArrayList<>(replicas);
        this.disabledReplicas = new boolean[replicas];

        for (int i = 0; i < replicas; i++) {
            storages.add(new DaoFileStorage(path.resolve("_replica_" + i)));
        }
    }

    @Override
    public byte[] get(String key) {
        return get(key, 1);
    }

    public byte[] get(String key, int ack) {
        List<byte[]> responses = new ArrayList<>();

        for (int i = 0; i < storages.size(); i++) {
            if (disabledReplicas[i]) {
                continue;
            }
            try {
                byte[] value = storages.get(i).readFromFile(key);
                responses.add(value);
            } catch (Exception e) {
                continue;
            }
        }

        if (responses.isEmpty()) {
            throw new NoSuchElementException("Элемент с ключом " + key + " не найден");
        }

        if (responses.size() < ack) {
            throw new IllegalStateException("Недостаточно реплик ответило");
        }

        byte[] winner = responses.get(0);
        int maxCount = 1;

        for (int i = 0; i < responses.size(); i++) {
            int count = 1;
            for (int j = i + 1; j < responses.size(); j++) {
                if (Arrays.equals(responses.get(i), responses.get(j))) {
                    count++;
                }
            }
            if (count > maxCount) {
                maxCount = count;
                winner = responses.get(i);
            }
        }

        return winner;
    }

    @Override
    public void upsert(String key, byte[] value) {
        upsert(key, value, 1);
    }

    public void upsert(String key, byte[] value, int ack) {
        int successCount = 0;
        for (int i = 0; i < storages.size(); i++) {
            if (disabledReplicas[i]) {
                continue;
            }
            try {
                storages.get(i).save(key, value);
                successCount++;
            } catch (Exception e) {
                continue;
            }
        }

        if (successCount < ack) {
            throw new IllegalStateException("Недостаточно реплик ответило");
        }
    }

    @Override
    public void delete(String key) {
        delete(key, 1);
    }

    public void delete(String key, int ack) {
        int successCount = 0;
        for (int i = 0; i < storages.size(); i++) {
            if (disabledReplicas[i]) {
                continue;
            }
            try {
                storages.get(i).deleteFromFile(key);
                successCount++;
            } catch (Exception e) {
                continue;
            }
        }

        if (successCount < ack) {
            throw new IllegalStateException("Недостаточно реплик ответило");
        }
    }

    @Override
    public void close() {
        for (DaoFileStorage storage : storages) {
            storage.dropStorage();
        }
    }

    public void disableReplica(int nodeId) {
        disabledReplicas[nodeId] = true;
    }

    public void enableReplica(int nodeId) {
        disabledReplicas[nodeId] = false;
    }

    public int getReplicaCount() {
        return storages.size();
    }
}
