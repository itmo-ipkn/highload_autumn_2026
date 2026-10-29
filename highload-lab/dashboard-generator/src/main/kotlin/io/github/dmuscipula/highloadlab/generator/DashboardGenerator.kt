package io.github.dmuscipula.highloadlab.generator

import java.net.HttpURLConnection.HTTP_OK
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Base64
import tools.jackson.databind.json.JsonMapper

private const val APPLICATION_JSON = "application/json"
private const val HANDLERS_API_PATH = "/handlers"
private const val GRAFANA_DASHBOARD_API_PATH = "/api/dashboards/db"

private val CONNECT_TIMEOUT = Duration.ofSeconds(5)
private val REQUEST_TIMEOUT = Duration.ofSeconds(10)
private val SUCCESS_HTTP_STATUS_CODES = 200..299

internal fun generateDashboard(config: GeneratorConfig) {
    val jsonMapper = JsonMapper.builder().build()

    HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build().use { httpClient ->
        val handlers = fetchHandlers(config.applicationBaseUrl, httpClient, jsonMapper)
        handlers.forEach(::println)

        val dashboard = createTemplateDashboard(handlers)
        val dashboardPath = saveDashboard(dashboard, config, httpClient, jsonMapper)
        println("Dashboard saved: $dashboardPath")
    }
}

private fun fetchHandlers(
    applicationBaseUrl: String,
    httpClient: HttpClient,
    jsonMapper: JsonMapper,
): List<String> {
    val handlersUri = URI.create("${applicationBaseUrl.trimEnd('/')}$HANDLERS_API_PATH")
    val request = HttpRequest.newBuilder(handlersUri)
        .header("Accept", APPLICATION_JSON)
        .timeout(REQUEST_TIMEOUT)
        .GET()
        .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() == HTTP_OK) {
        "GET $handlersUri failed: HTTP ${response.statusCode()}"
    }

    val handlersJson = jsonMapper.readTree(response.body())
    check(handlersJson.isArray) { "Expected /handlers to return a JSON array" }

    return handlersJson.toList().map { handlerJson ->
        val method = handlerJson.required("method").asString()
        val path = handlerJson.required("path").asString()
        "$method $path"
    }
}

private fun saveDashboard(
    dashboard: Map<String, Any>,
    config: GeneratorConfig,
    httpClient: HttpClient,
    jsonMapper: JsonMapper,
): String {
    val requestBody = mapOf(
        "dashboard" to dashboard,
        "overwrite" to true,
    )
    val encodedCredentials = Base64.getEncoder()
        .encodeToString("${config.grafanaUsername}:${config.grafanaPassword}".toByteArray(Charsets.UTF_8))
    val request = HttpRequest.newBuilder(URI.create("${config.grafanaBaseUrl}$GRAFANA_DASHBOARD_API_PATH"))
        .header("Authorization", "Basic $encodedCredentials")
        .header("Content-Type", APPLICATION_JSON)
        .header("Accept", APPLICATION_JSON)
        .timeout(REQUEST_TIMEOUT)
        .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(requestBody)))
        .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() in SUCCESS_HTTP_STATUS_CODES) {
        "Saving dashboard failed: HTTP ${response.statusCode()}: ${response.body()}"
    }

    return jsonMapper.readTree(response.body()).required("url").asString()
}
