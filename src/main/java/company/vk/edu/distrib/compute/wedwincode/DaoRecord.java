package company.vk.edu.distrib.compute.wedwincode;

public record DaoRecord(byte[] data, long timestamp, boolean deleted) {

    public static DaoRecord buildDeleted() {
        return new DaoRecord(null, System.currentTimeMillis(), true);
    }

    public static DaoRecord buildCreated(byte[] data) {
        return new DaoRecord(data, System.currentTimeMillis(), false);
    }

}
