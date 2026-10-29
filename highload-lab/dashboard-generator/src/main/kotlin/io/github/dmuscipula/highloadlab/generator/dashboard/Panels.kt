package io.github.dmuscipula.highloadlab.generator.dashboard

import com.grafana.foundation.dashboard.DataSourceRef
import com.grafana.foundation.prometheus.DataqueryBuilder
import com.grafana.foundation.prometheus.PromQueryFormat
import com.grafana.foundation.timeseries.TimeseriesPanelBuilder
import com.grafana.foundation.units.Constants.RequestsPerSecond
import com.grafana.foundation.units.Constants.Seconds
import io.github.dmuscipula.highloadlab.generator.model.Handler

private const val PROMETHEUS_DATASOURCE_UID = "prometheus"
private const val GRAFANA_GRID_COLUMNS = 24
private const val PANELS_PER_ROW = 3
private const val TIME_SERIES_PANEL_HEIGHT = 8
private const val DEFAULT_QUERY_REF_ID = "A"
private const val RPS_LABEL = "RPS"
private const val ERRORS_LABEL = "Errors"
private const val LATENCY_LABEL = "Latency"

internal fun createRpsPanel(handler: Handler, panelId: Int): TimeseriesPanelBuilder =
    createTimeSeriesPanel(panelId, title = "$RPS_LABEL: ${handler.method} ${handler.path}")
        .unit(RequestsPerSecond)
        .withTarget(
            createPrometheusQuery(
                expression = buildRpsExpression(handler),
                legend = RPS_LABEL,
            ),
        )

internal fun createErrorsPanel(handler: Handler, panelId: Int): TimeseriesPanelBuilder =
    createTimeSeriesPanel(panelId, title = "$ERRORS_LABEL: ${handler.method} ${handler.path}")
        .unit(RequestsPerSecond)
        .withTarget(
            createPrometheusQuery(
                expression = buildErrorsExpression(handler),
                legend = "{{status}}",
            ),
        )

internal fun createLatencyPanel(handler: Handler, panelId: Int): TimeseriesPanelBuilder =
    createTimeSeriesPanel(panelId, title = "$LATENCY_LABEL: ${handler.method} ${handler.path}")
        .unit(Seconds)
        .withTarget(
            createPrometheusQuery(
                expression = buildLatencyExpression(handler, quantile = 0.5),
                legend = "p50",
            ),
        )
        .withTarget(
            createPrometheusQuery(
                expression = buildLatencyExpression(handler, quantile = 0.95),
                legend = "p95",
                refId = "B",
            ),
        )
        .withTarget(
            createPrometheusQuery(
                expression = buildLatencyExpression(handler, quantile = 0.99),
                legend = "p99",
                refId = "C",
            ),
        )

private fun createTimeSeriesPanel(panelId: Int, title: String): TimeseriesPanelBuilder =
    TimeseriesPanelBuilder()
        .id(panelId)
        .title(title)
        .span(GRAFANA_GRID_COLUMNS / PANELS_PER_ROW)
        .height(TIME_SERIES_PANEL_HEIGHT)
        .datasource(createPrometheusDataSource())
        .min(0.0)

private fun createPrometheusQuery(
    expression: String,
    legend: String,
    refId: String = DEFAULT_QUERY_REF_ID,
): DataqueryBuilder =
    DataqueryBuilder()
        .refId(refId)
        .datasource(createPrometheusDataSource())
        .expr(expression)
        .range()
        .format(PromQueryFormat.TIME_SERIES)
        .legendFormat(legend)

internal fun createPrometheusDataSource(): DataSourceRef = DataSourceRef().apply {
    type = "prometheus"
    uid = PROMETHEUS_DATASOURCE_UID
}
