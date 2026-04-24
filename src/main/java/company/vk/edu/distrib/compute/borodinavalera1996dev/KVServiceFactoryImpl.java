package company.vk.edu.distrib.compute.borodinavalera1996dev;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.h2.util.StringUtils;

import java.io.IOException;
import java.nio.file.Path;

import static company.vk.edu.distrib.compute.borodinavalera1996dev.cluster.KVClusterFactoryImpl.NUMBER_OF_REPLICATION;

public class KVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, Path.of("storage","borodinavalera1996dev"), getNumberOfReplications());
    }

    private static int getNumberOfReplications() {
        String numberOfReplications = System.getProperty("numberOfReplications");
        if (StringUtils.isNullOrEmpty(numberOfReplications)) {
            return NUMBER_OF_REPLICATION;
        } else {
            return Integer.parseInt(numberOfReplications);
        }
    }
}
