package company.vk.edu.distrib.compute.nihuaway00.replication;

import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicaNode {
    private final int nodeId;
    private final EntityDao dao;
    private final AtomicBoolean enabled;

    public ReplicaNode(int nodeId, EntityDao dao) {
        this.enabled = new AtomicBoolean(dao.available());
        this.dao = dao;
        this.nodeId = nodeId;
    }

    public boolean getEnabled() {
        return enabled.get();
    }

    public void disable() {
        this.enabled.set(false);
    }

    public void enable() {
        this.enabled.set(true);
    }

    public EntityDao dao() {
        return dao;
    }

    public int nodeId() {
        return nodeId;
    }
}
