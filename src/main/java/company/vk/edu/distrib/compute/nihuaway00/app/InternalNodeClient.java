package company.vk.edu.distrib.compute.nihuaway00.app;

public interface InternalNodeClient {
    byte[] get(String grpcEndpoint, String key, int ack);

    void put(String grpcEndpoint, String key, byte[] value, int
            ack);

    void delete(String grpcEndpoint, String key, int ack);
}
