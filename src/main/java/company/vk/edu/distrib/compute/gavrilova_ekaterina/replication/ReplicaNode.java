package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.FileDao;

import java.io.IOException;
import java.nio.file.Path;

public class ReplicaNode {

    final Dao<byte[]> dao;
    boolean alive = true;

    ReplicaNode(int id) throws IOException {
        this.dao = new FileDao(Path.of("storage-node-" + id));
    }

}
