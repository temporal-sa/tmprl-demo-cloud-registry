package com.quickmeals.delivery.matching.repository

import com.quickmeals.delivery.matching.model.Dasher
import com.quickmeals.delivery.matching.model.GeoPoint
import com.quickmeals.delivery.matching.scoring.Haversine
import java.util.concurrent.ConcurrentHashMap

/**
 * In-memory live view of eligible Dashers, keyed by region. In production this is
 * backed by the fleet-state stream and a region grid index; the grid lets us
 * pre-filter candidates before scoring so we stay inside the latency budget.
 */
class DasherRepository {
    private val byRegion = ConcurrentHashMap<String, MutableList<Dasher>>()

    fun upsert(regionId: String, dasher: Dasher) {
        val list = byRegion.getOrPut(regionId) { mutableListOf() }
        list.removeIf { it.id == dasher.id }
        if (dasher.onShift) list.add(dasher)
    }

    fun candidates(regionId: String, near: GeoPoint, radiusMeters: Double, limit: Int): List<Dasher> {
        val pool = byRegion[regionId] ?: return emptyList()
        return pool.asSequence()
            .filter { it.onShift }
            .filter { Haversine.distanceMeters(it.location, near) <= radiusMeters }
            .sortedBy { Haversine.distanceMeters(it.location, near) }
            .take(limit)
            .toList()
    }
}
