plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.grafana:grafana-foundation-sdk:11.6.0-1769699379")
    implementation("tools.jackson.core:jackson-databind:3.1.7")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "io.github.dmuscipula.highloadlab.generator.MainKt"
}
