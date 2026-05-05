package company.vk.edu.distrib.compute.andeco.election;

public record ElectionNodeParameters(long pingTimeoutMs,
                                     long electionAnswerTimeoutMs,
                                     long victoryTimeoutMs,
                                     double failProbabilityPerTick,
                                     long minRecoverMs,
                                     long maxRecoverMs,
                                     long randomSeed) {
}
