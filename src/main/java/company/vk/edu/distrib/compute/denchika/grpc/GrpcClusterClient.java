package company.vk.edu.distrib.compute.denchika.grpc;

import company.vk.edu.distrib.compute.proto.denchika.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GrpcClusterClient {

    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public byte[] get(String host, int port, String id) {
        GetResponse response = stub(host, port).get(GetRequest.newBuilder().setId(id).build());
        if (!response.getFound()) {
            throw new NoSuchElementException(id);
        }
        return response.getValue().toByteArray();
    }

    @SuppressWarnings("unused")
    public void upsert(String host, int port, String key, byte[] value) {
        PutResponse upsert = stub(host, port).upsert(PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build());
    }

    @SuppressWarnings("unused")
    public void delete(String host, int port, String id) {
        DeleteResponse delete = stub(host, port).delete(DeleteRequest.newBuilder().setId(id).build());
    }

    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }

    private InternalKVServiceGrpc.InternalKVServiceBlockingStub stub(String host, int port) {
        return InternalKVServiceGrpc.newBlockingStub(
            channels.computeIfAbsent(
                host + ":" + port,
                addr -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
            )
        );
    }
}
