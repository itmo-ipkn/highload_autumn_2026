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
    runtimeOnly("org.postgresql:postgresql:42.7.14")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "io.github.dmuscipula.kafkalab.ProducerKt"
}
