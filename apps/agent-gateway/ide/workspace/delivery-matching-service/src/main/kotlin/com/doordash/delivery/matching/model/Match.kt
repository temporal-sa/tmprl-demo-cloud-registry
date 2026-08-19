package com.quickmeals.delivery.matching.model

data class ScoredCandidate(
    val dasher: Dasher,
    val score: Double,
    val projectedEtaSeconds: Int,
)

data class MatchDecision(
    val deliveryId: String,
    val dasherId: String,
    val score: Double,
    val projectedEtaSeconds: Int,
    val batched: Boolean = false,
    val batchedDeliveryIds: List<String> = emptyList(),
)
