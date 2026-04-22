package company.vk.edu.distrib.compute.wedwincode.sharded;

import java.util.zip.CRC32;

/**
 * This interface determines what node is responsible for handling certain entity id.
 */
@FunctionalInterface
public interface HashStrategy {
    String getEndpoint(String id);

    default long hash(String input) {
        CRC32 crc = new CRC32();
        crc.update(input.getBytes());
        return crc.getValue();
    }
}
