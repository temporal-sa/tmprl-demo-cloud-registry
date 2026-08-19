package com.quickmeals.delivery.matching.grpc

import com.quickmeals.delivery.matching.engine.MatchingEngine
import com.quickmeals.delivery.matching.model.DeliveryRequest
import com.quickmeals.delivery.matching.model.GeoPoint
import org.slf4j.LoggerFactory

/**
 * Thin gRPC adapter over the matching engine. Generated protobuf types are mapped
 * to and from the domain models so the engine stays transport agnostic.
 */
class MatchingGrpcService(private val engine: MatchingEngine) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun match(request: DeliveryRequest) {
        val decision = engine.match(request)
        if (decision == null) {
            log.info("no match for delivery {}", request.deliveryId)
            return
        }
        log.info(
            "matched delivery {} to dasher {} score={} eta={}s",
            decision.deliveryId,
            decision.dasherId,
            decision.score,
            decision.projectedEtaSeconds,
        )
    }
}
