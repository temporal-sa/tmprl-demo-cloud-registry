package com.quickmeals.delivery.matching.model

data class GeoPoint(val lat: Double, val lng: Double)

data class Dasher(
    val id: String,
    val location: GeoPoint,
    val activeDeliveries: Int,
    val acceptanceRate: Double,
    val onShift: Boolean,
    val vehicle: Vehicle,
)

enum class Vehicle { CAR, BIKE, SCOOTER, WALK }
