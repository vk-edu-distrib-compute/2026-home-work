package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Wolfram158KVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        try {
            return new Wolfram158KVClusterImpl(
                    Utils.mapToLocalhostEndpoints(ports),
                    new Wolfram158KVServiceFactoryFileWithCacheImpl()
            );
        } catch (IOException | NoSuchAlgorithmException ignored) {
            return null;
        }
    }
}
