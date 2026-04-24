package company.vk.edu.distrib.compute.borodinavalera1996dev.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.borodinavalera1996dev.FileDao;
import company.vk.edu.distrib.compute.borodinavalera1996dev.FileStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicaNode {

    private final Dao<FileStorage.Data> dao;
    private final int id;
    private AtomicBoolean isAlive = new AtomicBoolean(true);

    public ReplicaNode(Path path, int id) throws IOException {
        this.id = id;
        this.dao = new FileDao(Path.of(path.toString(), "replication-node-" + id));
    }

    public Dao<FileStorage.Data> getDao() {
        return dao;
    }

    public AtomicBoolean getIsAlive() {
        return isAlive;
    }

    public void setIsAlive(AtomicBoolean isAlive) {
        this.isAlive = isAlive;
    }

    public int getId() {
        return id;
    }
}
