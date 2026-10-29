package io.github.dmuscipula.highloadlab.generator.dashboard

import io.github.dmuscipula.highloadlab.generator.model.Handler

private const val PROMETHEUS_JOB_NAME = "highload-lab"
private const val HTTP_REQUEST_COUNT_METRIC = "http_server_requests_seconds_count"
private const val HTTP_REQUEST_DURATION_BUCKET_METRIC = "http_server_requests_seconds_bucket"
private const val HTTP_ERROR_STATUS_PATTERN = "4..|5.."
private const val GRAFANA_RATE_INTERVAL = $$"$__rate_interval"

internal fun buildRpsExpression(handler: Handler): String {
    val labelMatchers = buildHandlerLabelMatchers(handler)

    return """
        sum(
          rate($HTTP_REQUEST_COUNT_METRIC{$labelMatchers}[$GRAFANA_RATE_INTERVAL])
        )
    """.trimIndent()
}

internal fun buildErrorsExpression(handler: Handler): String {
    val labelMatchers = buildHandlerLabelMatchers(handler)

    return """
        sum by (status) (
          rate($HTTP_REQUEST_COUNT_METRIC{$labelMatchers,status=~"$HTTP_ERROR_STATUS_PATTERN"}[$GRAFANA_RATE_INTERVAL])
        )
    """.trimIndent()
}

internal fun buildLatencyExpression(handler: Handler, quantile: Double): String {
    val labelMatchers = buildHandlerLabelMatchers(handler)

    return """
        histogram_quantile(
          $quantile,
          sum by (le) (
            rate($HTTP_REQUEST_DURATION_BUCKET_METRIC{$labelMatchers}[$GRAFANA_RATE_INTERVAL])
          )
        )
    """.trimIndent()
}

private fun buildHandlerLabelMatchers(handler: Handler): String =
    "job=\"$PROMETHEUS_JOB_NAME\",method=\"${handler.method}\",uri=\"${handler.path}\""
