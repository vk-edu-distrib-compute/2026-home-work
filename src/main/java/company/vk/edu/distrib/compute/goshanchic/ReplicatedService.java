package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVService;

public interface ReplicatedService extends KVService {

    int getReplicationFactor();

    int getAck();

    void setAck(int ack);
}

