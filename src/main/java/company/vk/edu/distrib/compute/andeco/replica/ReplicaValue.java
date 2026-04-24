package company.vk.edu.distrib.compute.andeco.replica;

import java.math.BigInteger;

public record ReplicaValue(byte[] value, BigInteger version, boolean deleted) {}
