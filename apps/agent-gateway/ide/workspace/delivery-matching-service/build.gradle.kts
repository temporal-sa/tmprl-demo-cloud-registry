plugins {
    kotlin("jvm") version "1.9.24"
    kotlin("plugin.serialization") version "1.9.24"
    id("com.google.protobuf") version "0.9.4"
    application
}

application {
    mainClass.set("com.quickmeals.delivery.matching.ApplicationKt")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("io.grpc:grpc-kotlin-stub:1.4.1")
    implementation("io.grpc:grpc-protobuf:1.64.0")
    implementation("io.grpc:grpc-netty-shaded:1.64.0")
    implementation("com.google.protobuf:protobuf-kotlin:3.25.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.13.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.1")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("ch.qos.logback:logback-classic:1.5.6")

    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:1.13.11")
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    useJUnitPlatform()
}
