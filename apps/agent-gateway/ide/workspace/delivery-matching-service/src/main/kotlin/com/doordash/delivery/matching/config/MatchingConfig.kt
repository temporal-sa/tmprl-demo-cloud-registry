package com.quickmeals.delivery.matching.config

data class ScoringWeights(
    val distance: Double,
    val eta: Double,
    val load: Double,
    val acceptance: Double,
)

data class MatchingConfig(
    val version: String,
    val grpcPort: Int,
    val candidateRadiusMeters: Double,
    val maxCandidates: Int,
    val scoring: ScoringWeights,
    val batchingEnabled: Boolean,
    val maxBatchSize: Int,
) {
    companion object {
        fun load(): MatchingConfig = MatchingConfig(
            version = "2.3.1",
            grpcPort = 8291,
            candidateRadiusMeters = 4000.0,
            maxCandidates = 64,
            scoring = ScoringWeights(0.45, 0.30, 0.15, 0.10),
            batchingEnabled = true,
            maxBatchSize = 3,
        )
    }
}
