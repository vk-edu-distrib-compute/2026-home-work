package company.vk.edu.distrib.compute.expanse.algorithm;

import company.vk.edu.distrib.compute.expanse.utils.ExceptionUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public final class RendezvousHashing {
    private RendezvousHashing() {

    }

    public static UUID getCorrespondingUid(String key, Set<UUID> uuids) {
        if (uuids.isEmpty()) {
            return null;
        }
        UUID bestUid = null;
        double bestWeight = -1.0;
        for (UUID uuid : uuids) {
            double weight = hashCombine(key, uuid.toString());
            if (weight > bestWeight) {
                bestWeight = weight;
                bestUid = uuid;
            }
        }
        return bestUid;
    }

    private static double hashCombine(String key, String uid) {
        try {
            String combined = key + ":" + uid;
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(combined.getBytes());
            long value = ((long) (digest[0] & 0xFF) << 56)
                    | ((long) (digest[1] & 0xFF) << 48)
                    | ((long) (digest[2] & 0xFF) << 40)
                    | ((long) (digest[3] & 0xFF) << 32)
                    | ((long) (digest[4] & 0xFF) << 24)
                    | ((long) (digest[5] & 0xFF) << 16)
                    | ((long) (digest[6] & 0xFF) << 8)
                    | (digest[7] & 0xFF);
            return (double) value / Long.MAX_VALUE; // Нормализация к [0,1)
        } catch (NoSuchAlgorithmException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }
}
