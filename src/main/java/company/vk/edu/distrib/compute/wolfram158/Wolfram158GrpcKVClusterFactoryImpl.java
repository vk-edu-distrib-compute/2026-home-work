package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Wolfram158GrpcKVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        try {
            return new Wolfram158GrpcKVClusterImpl(
                    Utils.mapToLocalhostEndpoints(ports),
                    new Wolfram158GrpcKVServiceFactoryFileWithCacheImpl()
            );
        } catch (IOException | NoSuchAlgorithmException ignored) {
            return null;
        }
    }
}
