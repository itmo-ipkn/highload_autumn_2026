plugins {
    kotlin("jvm") version "2.3.21"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.1.2")
    implementation("tools.jackson.core:jackson-databind:3.1.7")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "io.github.dmuscipula.kafkalab.MainKt"
}
