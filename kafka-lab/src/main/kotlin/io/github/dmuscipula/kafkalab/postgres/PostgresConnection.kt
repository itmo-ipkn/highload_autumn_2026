package io.github.dmuscipula.kafkalab.postgres

import java.sql.Connection
import java.sql.DriverManager

internal fun openPostgresConnection(): Connection {
    val url = "jdbc:postgresql://postgres:5432/kafka_lab"
    val password = System.getenv("POSTGRES_PASSWORD") ?: "kafka-lab-local"
    return DriverManager.getConnection(url, "kafka_lab", password)
}
