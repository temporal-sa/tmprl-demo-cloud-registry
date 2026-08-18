package com.quickmeals.delivery.matching.engine

import com.quickmeals.delivery.matching.config.MatchingConfig
import com.quickmeals.delivery.matching.model.Dasher
import com.quickmeals.delivery.matching.model.DeliveryRequest
import com.quickmeals.delivery.matching.model.GeoPoint
import com.quickmeals.delivery.matching.model.Vehicle
import com.quickmeals.delivery.matching.repository.DasherRepository
import com.quickmeals.delivery.matching.scoring.ScoringModel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class MatchingEngineTest {

    private val config = MatchingConfig.load()

    private fun engine(): Pair<MatchingEngine, DasherRepository> {
        val repo = DasherRepository()
        val engine = MatchingEngine(repo, ScoringModel(config.scoring), config)
        return engine to repo
    }

    @Test
    fun `prefers the closer idle dasher`() {
        val (engine, repo) = engine()
        repo.upsert("sf-bay", Dasher("d_near", GeoPoint(37.7750, -122.4195), 0, 0.95, true, Vehicle.BIKE))
        repo.upsert("sf-bay", Dasher("d_far", GeoPoint(37.8100, -122.3800), 2, 0.90, true, Vehicle.CAR))

        val decision = engine.match(
            DeliveryRequest("dlv_1", "sf-bay", GeoPoint(37.7749, -122.4194), GeoPoint(37.79, -122.41), 300, 24.0)
        )

        assertNotNull(decision)
        assertEquals("d_near", decision.dasherId)
    }

    @Test
    fun `returns null when no dashers are on shift`() {
        val (engine, _) = engine()
        val decision = engine.match(
            DeliveryRequest("dlv_2", "sf-bay", GeoPoint(37.7749, -122.4194), GeoPoint(37.79, -122.41), 300, 24.0)
        )
        assertEquals(null, decision)
    }
}
