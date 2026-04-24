package company.vk.edu.distrib.compute.ce_fello;

final class CeFelloInsufficientAckException extends RuntimeException {
    CeFelloInsufficientAckException(String message) {
        super(message);
    }
}
