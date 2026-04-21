package company.vk.edu.distrib.compute.ce_fello;

enum CeFelloDistributionMode {
    RENDEZVOUS("rendezvous"),
    CONSISTENT("consistent");

    private static final String PROPERTY_NAME = "ce_fello.distribution";

    private final String propertyValue;

    CeFelloDistributionMode(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    static CeFelloDistributionMode fromSystemProperty() {
        String rawValue = System.getProperty(PROPERTY_NAME);
        if (rawValue == null || rawValue.isBlank()) {
            return RENDEZVOUS;
        }

        for (CeFelloDistributionMode mode : values()) {
            if (mode.propertyValue.equalsIgnoreCase(rawValue)) {
                return mode;
            }
        }

        throw new IllegalArgumentException("Unsupported distribution mode: " + rawValue);
    }

    CeFelloKeyDistributor newDistributor(Iterable<String> endpoints) {
        return switch (this) {
            case RENDEZVOUS -> new CeFelloRendezvousKeyDistributor(endpoints);
            case CONSISTENT -> new CeFelloConsistentHashKeyDistributor(endpoints);
        };
    }
}
