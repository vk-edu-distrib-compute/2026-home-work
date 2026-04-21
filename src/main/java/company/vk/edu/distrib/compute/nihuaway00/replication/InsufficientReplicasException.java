package company.vk.edu.distrib.compute.nihuaway00.replication;

public class InsufficientReplicasException extends RuntimeException {
    public InsufficientReplicasException(){
        super();
    }

    public InsufficientReplicasException (String message){
        super(message);
    }
}
