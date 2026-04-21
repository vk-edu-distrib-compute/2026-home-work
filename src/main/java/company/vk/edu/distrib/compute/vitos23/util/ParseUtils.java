package company.vk.edu.distrib.compute.vitos23.util;

public final class ParseUtils {
    private ParseUtils() {
    }

    public static Integer parseInteger(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
