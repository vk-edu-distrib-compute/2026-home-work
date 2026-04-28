package company.vk.edu.distrib.compute.mediocritas.cluster.proxy;

import java.time.Duration;

public record ProxyConfig(Duration connectTimeout, Duration requestTimeout) {

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(5);

    public static ProxyConfig defaultConfig() {
        return new ProxyConfig(DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
    }
}
