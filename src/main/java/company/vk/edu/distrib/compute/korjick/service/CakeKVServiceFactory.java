package company.vk.edu.distrib.compute.korjick.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.korjick.dao.H2Dao;
import company.vk.edu.distrib.compute.korjick.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.http.entity.LocalEntityRequestProcessor;
import company.vk.edu.distrib.compute.korjick.http.status.StatusHandler;

import java.io.IOException;

public class CakeKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        var dao = new H2Dao(port);
        var service = new CakeKVService("localhost", port, dao);
        var requestProcessor = new LocalEntityRequestProcessor(dao);
        service.addContext("/v0/status", new StatusHandler());
        service.addContext("/v0/entity", new EntityHandler(requestProcessor));
        return service;
    }
}
