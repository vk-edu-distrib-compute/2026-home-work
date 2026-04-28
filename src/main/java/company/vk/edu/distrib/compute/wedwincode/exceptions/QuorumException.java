package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class QuorumException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 2383443652534261203L;

    public QuorumException(String message) {
        super(message);
    }
}
