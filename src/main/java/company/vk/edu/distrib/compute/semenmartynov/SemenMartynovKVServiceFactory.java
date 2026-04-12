package company.vk.edu.distrib.compute.semenmartynov;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

/**
 * Factory for creating instances of {@link SemenMartynovKVService}.
 */
public class SemenMartynovKVServiceFactory extends KVServiceFactory {

    /**
     * Creates a new instance of {@link SemenMartynovKVService} bound to the specified port.
     *
     * @param port the port to bind the HTTP server to
     * @return an instance of {@link KVService}
     * @throws IOException if the underlying server fails to bind to the port
     */
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new SemenMartynovKVService(port);
    }
}
