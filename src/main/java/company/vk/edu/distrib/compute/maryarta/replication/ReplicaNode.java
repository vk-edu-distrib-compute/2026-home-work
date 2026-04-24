package company.vk.edu.distrib.compute.maryarta.replication;

import company.vk.edu.distrib.compute.maryarta.H2Dao;

public class ReplicaNode {
    private final H2Dao dao;
    private boolean enabled = true;

    public ReplicaNode(H2Dao dao){
        this.dao = dao;
    }


    public H2Dao dao() {
        return dao;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void disable() {
        enabled = false;
        dao.close();
    }

    public void enable() {
        if (!enabled) {
            dao.start();
            enabled = true;
        }
    }
}
