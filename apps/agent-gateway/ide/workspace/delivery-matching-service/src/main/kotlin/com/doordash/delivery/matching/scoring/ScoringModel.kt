package com.quickmeals.delivery.matching.scoring

import com.quickmeals.delivery.matching.config.ScoringWeights
import com.quickmeals.delivery.matching.model.Dasher
import com.quickmeals.delivery.matching.model.DeliveryRequest
import kotlin.math.max

/**
 * Weighted linear score in [0, 1]. Higher is a better match. Favors short pickup
 * distance and ETA, penalizes current load, rewards a healthy acceptance rate.
 * Tuned offline against historical assignment outcomes.
 */
class ScoringModel(private val weights: ScoringWeights) {

    private companion object {
        const val TARGET_PICKUP_METERS = 4000.0
        const val TARGET_ETA_SECONDS = 900.0
        const val MAX_ACTIVE_DELIVERIES = 4.0
        const val METERS_PER_SECOND = 6.5
    }

    fun score(request: DeliveryRequest, dasher: Dasher): Double {
        val pickupDistance = Haversine.distanceMeters(dasher.location, request.pickup)
        val etaSeconds = projectedEtaSeconds(request, dasher).toDouble()

        val distanceScore = 1.0 - clamp(pickupDistance / TARGET_PICKUP_METERS)
        val etaScore = 1.0 - clamp(etaSeconds / TARGET_ETA_SECONDS)
        val loadScore = 1.0 - clamp(dasher.activeDeliveries / MAX_ACTIVE_DELIVERIES)
        val acceptanceScore = clamp(dasher.acceptanceRate)

        return weights.distance * distanceScore +
            weights.eta * etaScore +
            weights.load * loadScore +
            weights.acceptance * acceptanceScore
    }

    fun projectedEtaSeconds(request: DeliveryRequest, dasher: Dasher): Int {
        val toPickup = Haversine.distanceMeters(dasher.location, request.pickup)
        val travel = toPickup / METERS_PER_SECOND
        return max(request.prepTimeSeconds.toDouble(), travel).toInt()
    }

    private fun clamp(v: Double): Double = v.coerceIn(0.0, 1.0)
}
