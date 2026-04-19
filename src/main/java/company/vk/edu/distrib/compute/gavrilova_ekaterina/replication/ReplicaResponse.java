package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

public record ReplicaResponse(int status, byte[] body) {

    public ReplicaResponse(int status, byte[] body) {
        this.status = status;
        this.body = body != null ? body.clone() : null;
    }

    public boolean isSuccess() {
        return status >= 200 && status < 300;
    }

}
