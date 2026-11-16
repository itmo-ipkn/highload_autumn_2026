package io.github.dmuscipula.highloadlab.load

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.random.Random
import kotlin.time.DurationUnit
import kotlin.time.TimeSource
import tools.jackson.databind.json.JsonMapper

private const val MISSING_ITEM_ID = -1L
private const val MISSING_GET_PROBABILITY = 0.05
private const val START_RPS = 2.0
private val HTTP_TIMEOUT = Duration.ofSeconds(3)
private val client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build()
private val mapper = JsonMapper.builder().build()

fun main(args: Array<String>) {
    require(args.isNotEmpty()) { "Usage: load-generator <application-url> [application-url ...]" }

    val urls = args.map { it.trimEnd('/') }
    val targetRps = System.getenv("LOAD_TARGET_RPS")?.toDouble() ?: 39.0
    val increaseSeconds = System.getenv("LOAD_INCREASE_SECONDS")?.toLong() ?: 120L

    require(targetRps.isFinite() && targetRps >= START_RPS) { "LOAD_TARGET_RPS must be at least $START_RPS" }
    require(increaseSeconds > 0) { "LOAD_INCREASE_SECONDS must be positive" }

    client.use {
        val requests = prepareRequests(urls)
        runLoad(
            requests = requests,
            targetRps = targetRps,
            increaseSeconds = increaseSeconds,
        )
    }
}

private fun prepareRequests(urls: List<String>): List<HttpRequest> =
    urls.flatMap { baseUrl ->
        val id = createItem(baseUrl)

        listOf(
            buildRequest("POST", "$baseUrl/items"),
            buildRequest("GET", "$baseUrl/items/$id"),
            buildRequest("PUT", "$baseUrl/items/$id"),
        )
    }

private fun createItem(baseUrl: String): Long {
    val request = buildRequest("POST", "$baseUrl/items")

    repeat(10) {
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() == 201) {
            return mapper.readTree(response.body()).required("id").asLong()
        }

        Thread.sleep(250)
    }

    error("Could not create an item at $baseUrl")
}

private fun buildRequest(method: String, url: String): HttpRequest {
    val body = if (method == "GET") {
        HttpRequest.BodyPublishers.noBody()
    } else {
        HttpRequest.BodyPublishers.ofString("""{"name":"load-item"}""")
    }

    return HttpRequest.newBuilder(URI.create(url))
        .timeout(HTTP_TIMEOUT)
        .header("Content-Type", "application/json")
        .method(method, body)
        .build()
}

private fun runLoad(requests: List<HttpRequest>, targetRps: Double, increaseSeconds: Long) {
    val start = TimeSource.Monotonic.markNow()
    var index = 0

    while (true) {
        val secondsSinceStart = start.elapsedNow().toDouble(DurationUnit.SECONDS)
        val rps = currentRps(
            secondsSinceStart = secondsSinceStart,
            targetRps = targetRps,
            increaseSeconds = increaseSeconds,
        )
        val template = requests[index]

        //For some 404
        val request = if (template.method() == "GET" && Random.nextDouble() < MISSING_GET_PROBABILITY) {
            val missingUrl = template.uri().resolve(MISSING_ITEM_ID.toString()).toString()
            buildRequest("GET", missingUrl)
        } else {
            template
        }

        client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            .exceptionally { error ->
                System.err.println("${request.method()} ${request.uri()}: ${error.message}")
                null
            }

        index = (index + 1) % requests.size

        Thread.sleep((1000 / rps).toLong())
    }
}

private fun currentRps(secondsSinceStart: Double, targetRps: Double, increaseSeconds: Long): Double {
    val increaseProgress = (secondsSinceStart / increaseSeconds).coerceAtMost(1.0)
    return START_RPS + (targetRps - START_RPS) * increaseProgress
}
