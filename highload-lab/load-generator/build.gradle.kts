plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("tools.jackson.core:jackson-databind:3.1.7")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "io.github.dmuscipula.highloadlab.load.MainKt"
}
