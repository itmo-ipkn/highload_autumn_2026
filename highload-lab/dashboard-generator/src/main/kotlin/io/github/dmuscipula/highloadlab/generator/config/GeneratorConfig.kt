package io.github.dmuscipula.highloadlab.generator.config

internal data class GeneratorConfig(
    val applicationBaseUrl: String,
    val grafanaBaseUrl: String,
    val grafanaUsername: String,
    val grafanaPassword: String,
)

internal fun readConfig(args: Array<String>): GeneratorConfig {
    val applicationBaseUrl = requireNotNull(args.singleOrNull()) {
        "Usage: dashboard-generator <application-url>"
    }

    return GeneratorConfig(
        applicationBaseUrl = applicationBaseUrl,
        grafanaBaseUrl = requiredEnvironment("GRAFANA_URL").trimEnd('/'),
        grafanaUsername = requiredEnvironment("GRAFANA_ADMIN_USER"),
        grafanaPassword = requiredEnvironment("GRAFANA_ADMIN_PASSWORD"),
    )
}

private fun requiredEnvironment(variableName: String): String =
    requireNotNull(System.getenv(variableName)?.takeIf { it.isNotBlank() }) {
        "Set $variableName environment variable"
    }
