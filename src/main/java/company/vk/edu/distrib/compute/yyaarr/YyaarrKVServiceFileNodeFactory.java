package company.vk.edu.distrib.compute.yyaarr;

import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;

public class YyaarrKVServiceFileNodeFactory extends YyaarrKVServiceFileFactory {
    private final YyaarrKVCluster cluster;

    public YyaarrKVServiceFileNodeFactory(YyaarrKVCluster cluster) {
        super();
        this.cluster = cluster;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new YyaarrKVServiceFile(port, new YyaarrFileDao(), cluster);
    }
}
