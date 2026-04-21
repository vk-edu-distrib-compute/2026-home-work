package company.vk.edu.distrib.compute.vitos23.shard;

public record ReplicaResult<T>(boolean success, T data) {
    public static <T> ReplicaResult<T> failed() {
        return new ReplicaResult<>(false, null);
    }

    public static <T> ReplicaResult<T> empty() {
        return new ReplicaResult<>(true, null);
    }
}
