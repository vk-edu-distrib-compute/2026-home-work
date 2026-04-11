package company.vk.edu.distrib.compute.patersss;

public class UrlParamsUtils {
    public static final String ID_PREFIX = "id=";
    public static String getIdFromQuery(String params) {
        if (params.startsWith(ID_PREFIX)) {
            return params.substring(ID_PREFIX.length());
        }
        throw new IllegalArgumentException("Bad query. There is no id param");
    }
}
