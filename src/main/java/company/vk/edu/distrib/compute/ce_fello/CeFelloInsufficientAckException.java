package company.vk.edu.distrib.compute.ce_fello;

final class CeFelloInsufficientAckException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    CeFelloInsufficientAckException(String message) {
        super(message);
    }
}
