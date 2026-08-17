package com.quickmeals.delivery.matching

import com.quickmeals.delivery.matching.config.MatchingConfig
import com.quickmeals.delivery.matching.engine.MatchingEngine
import com.quickmeals.delivery.matching.grpc.MatchingGrpcService
import com.quickmeals.delivery.matching.repository.DasherRepository
import com.quickmeals.delivery.matching.scoring.ScoringModel
import io.grpc.ServerBuilder
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("delivery-matching-service")

fun main() {
    val config = MatchingConfig.load()
    log.info("starting delivery-matching-service v${config.version} on :${config.grpcPort}")

    val dashers = DasherRepository()
    val scoring = ScoringModel(config.scoring)
    val engine = MatchingEngine(dashers, scoring, config)

    val server = ServerBuilder
        .forPort(config.grpcPort)
        .addService(MatchingGrpcService(engine))
        .build()

    server.start()
    log.info("matching service ready")
    Runtime.getRuntime().addShutdownHook(Thread { server.shutdown() })
    server.awaitTermination()
}
