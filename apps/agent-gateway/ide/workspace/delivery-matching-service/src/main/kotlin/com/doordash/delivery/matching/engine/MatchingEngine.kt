package com.quickmeals.delivery.matching.engine

import com.quickmeals.delivery.matching.config.MatchingConfig
import com.quickmeals.delivery.matching.model.DeliveryRequest
import com.quickmeals.delivery.matching.model.MatchDecision
import com.quickmeals.delivery.matching.model.ScoredCandidate
import com.quickmeals.delivery.matching.repository.DasherRepository
import com.quickmeals.delivery.matching.scoring.ScoringModel
import org.slf4j.LoggerFactory

class MatchingEngine(
    private val dashers: DasherRepository,
    private val scoring: ScoringModel,
    private val config: MatchingConfig,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun match(request: DeliveryRequest): MatchDecision? {
        val candidates = dashers.candidates(
            regionId = request.regionId,
            near = request.pickup,
            radiusMeters = config.candidateRadiusMeters,
            limit = config.maxCandidates,
        )
        if (candidates.isEmpty()) {
            log.debug("no eligible dashers for delivery {}", request.deliveryId)
            return null
        }

        val best = candidates
            .map { d ->
                ScoredCandidate(
                    dasher = d,
                    score = scoring.score(request, d),
                    projectedEtaSeconds = scoring.projectedEtaSeconds(request, d),
                )
            }
            .maxByOrNull { it.score } ?: return null

        return MatchDecision(
            deliveryId = request.deliveryId,
            dasherId = best.dasher.id,
            score = best.score,
            projectedEtaSeconds = best.projectedEtaSeconds,
        )
    }
}
