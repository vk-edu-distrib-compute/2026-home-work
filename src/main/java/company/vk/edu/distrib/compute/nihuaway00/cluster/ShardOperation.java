package company.vk.edu.distrib.compute.nihuaway00.cluster;

import company.vk.edu.distrib.compute.nihuaway00.proto.KVServiceGrpc;

@FunctionalInterface
public interface ShardOperation<T> {
    T execute(KVServiceGrpc.KVServiceBlockingStub stub);
}
