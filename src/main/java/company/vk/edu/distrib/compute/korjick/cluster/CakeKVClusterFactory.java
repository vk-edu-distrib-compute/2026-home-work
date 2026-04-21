package company.vk.edu.distrib.compute.korjick.cluster;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.dao.H2Dao;
import company.vk.edu.distrib.compute.korjick.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.http.entity.RendezvousEntityRequestProcessor;
import company.vk.edu.distrib.compute.korjick.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.service.CakeKVService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CakeKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, MapData> mapData = new ConcurrentHashMap<>();

        try {
            for (int port : ports) {
                var dao = new H2Dao(port);
                var service = new CakeKVService("localhost", port, dao);
                mapData.put(service.getEndpoint(), new MapData(service, dao));
            }

            var endpoints = mapData.keySet().stream().toList();
            for (var data : mapData.values()) {
                var requestProcessor =
                        new RendezvousEntityRequestProcessor(data.dao,
                                data.service.getEndpoint(),
                                endpoints);
                data.service.addContext("/v0/status", new StatusHandler());
                data.service.addContext("/v0/entity", new EntityHandler(requestProcessor));
            }

            Map<String, KVService> services = new ConcurrentHashMap<>();
            for (var data : mapData.values()) {
                services.put(data.service.getEndpoint(), data.service);
            }
            return new CakeKVCluster(services);
        } catch (Exception e) {
            for (var data : mapData.values()) {
                data.service.stop();
            }
            throw new RuntimeException("Failed to create cluster", e);
        }
    }

    private record MapData(CakeKVService service, Dao<byte[]> dao) {
    }
}
