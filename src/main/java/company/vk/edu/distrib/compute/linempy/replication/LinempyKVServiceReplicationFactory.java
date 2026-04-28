package company.vk.edu.distrib.compute.linempy.replication;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

/**
 * Фабрика реплицированного сервиса.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class LinempyKVServiceReplicationFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceReplicationImpl(port);
    }
}
