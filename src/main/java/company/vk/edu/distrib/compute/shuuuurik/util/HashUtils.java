package company.vk.edu.distrib.compute.shuuuurik.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashUtils {
    private HashUtils() {
    }

    /**
     * Вычисляет хэш пары (node, key).
     * Использует MD5 и берёт первые 8 байт как long.
     *
     * @param node endpoint ноды
     * @param key  ключ запроса
     * @return числовой хэш для данной пары
     */
    public static long hashPair(String node, String key) {
        String combined = node + ":" + key; // разделитель необходим
        byte[] md = md5(combined.getBytes(StandardCharsets.UTF_8));

        long result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (md[i] & 0xFF);
        }
        return result;
    }

    /**
     * Хэширует строку в пространство [0, 2^32) через MD5.
     * Возвращает беззнаковый 32-битный int.
     *
     * @param value строка для хэширования
     * @return позиция на кольце [0, 2^32)
     */
    public static long hashToRing(String value) {
        byte[] md5 = md5(value.getBytes(StandardCharsets.UTF_8));
        // Берёт первые 4 байта как беззнаковый int
        return ((long) (md5[0] & 0xFF) << 24)
                | ((long) (md5[1] & 0xFF) << 16)
                | ((long) (md5[2] & 0xFF) << 8)
                | ((md5[3] & 0xFF));
    }

    /**
     * Возвращает MD5-хэш массива байт.
     * Подходит для равномерного распределения в хэш-таблицах.
     */
    private static byte[] md5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }
}
