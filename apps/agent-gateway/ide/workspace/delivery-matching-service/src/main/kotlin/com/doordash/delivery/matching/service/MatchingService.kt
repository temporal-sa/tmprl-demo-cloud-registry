package com.quickmeals.delivery.matching.service

import com.quickmeals.delivery.matching.engine.MatchingEngine
import com.quickmeals.delivery.matching.model.DeliveryRequest
import com.quickmeals.delivery.matching.model.MatchDecision
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Application-level entry point for matching. Wraps the engine with the scoring
 * dispatcher and is where metrics and tracing hooks are attached.
 */
class MatchingService(private val engine: MatchingEngine) {

    suspend fun match(request: DeliveryRequest): MatchDecision? =
        withContext(Dispatchers.Default) {
            engine.match(request)
        }
}
