package com.quickmeals.delivery.matching.model

data class DeliveryRequest(
    val deliveryId: String,
    val regionId: String,
    val pickup: GeoPoint,
    val dropoff: GeoPoint,
    val prepTimeSeconds: Int,
    val orderValueUsd: Double,
)
