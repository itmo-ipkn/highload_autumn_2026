package io.github.dmuscipula.highloadlab.generator.dashboard

import com.grafana.foundation.common.Constants
import com.grafana.foundation.dashboard.Dashboard
import com.grafana.foundation.dashboard.DashboardBuilder
import com.grafana.foundation.dashboard.DashboardDashboardTimeBuilder
import com.grafana.foundation.dashboard.QueryVariableBuilder
import com.grafana.foundation.dashboard.StringOrArrayOfString
import com.grafana.foundation.dashboard.StringOrMap
import com.grafana.foundation.dashboard.VariableOption
import com.grafana.foundation.dashboard.VariableRefresh
import com.grafana.foundation.dashboard.VariableSort
import io.github.dmuscipula.highloadlab.generator.client.fetchHandlers
import io.github.dmuscipula.highloadlab.generator.client.saveDashboard
import io.github.dmuscipula.highloadlab.generator.config.GeneratorConfig
import io.github.dmuscipula.highloadlab.generator.model.Handler
import java.net.http.HttpClient
import java.time.Duration
import tools.jackson.databind.json.JsonMapper

private const val DASHBOARD_UID = "highload-lab-generated"
private const val DASHBOARD_TITLE = "Highload lab"
private const val DEFAULT_TIME_RANGE_FROM = "now-15m"
private const val DEFAULT_TIME_RANGE_TO = "now"
private const val DASHBOARD_REFRESH_INTERVAL = "5s"

private val CONNECT_TIMEOUT = Duration.ofSeconds(5)

internal fun generateDashboard(config: GeneratorConfig) {
    val jsonMapper = JsonMapper.builder().build()

    HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build().use { httpClient ->
        val handlers = fetchHandlers(config.applicationBaseUrl, httpClient, jsonMapper)
        handlers.forEach { handler -> println("${handler.method} ${handler.path}") }

        val dashboard = createDashboard(handlers)
        val dashboardPath = saveDashboard(dashboard, config, httpClient, jsonMapper)
        println("Dashboard saved: $dashboardPath")
    }
}

private fun createDashboard(handlers: List<Handler>): Dashboard {
    val dashboardBuilder = DashboardBuilder(DASHBOARD_TITLE)
        .uid(DASHBOARD_UID)
        .readonly()
        .tags(listOf("generated from kotlin"))
        .time(
            DashboardDashboardTimeBuilder()
                .from(DEFAULT_TIME_RANGE_FROM)
                .to(DEFAULT_TIME_RANGE_TO),
        )
        .timezone(Constants.TimeZoneBrowser)
        .refresh(DASHBOARD_REFRESH_INTERVAL)
        .withVariable(
            createLabelVariable(
                name = "datacenter",
                label = "Datacenter",
                metricSelector = "$HTTP_REQUEST_COUNT_METRIC{job=\"$PROMETHEUS_JOB_NAME\"}",
            ),
        )
        .withVariable(
            createLabelVariable(
                name = "pod_id",
                label = "Pod",
                metricSelector = "$HTTP_REQUEST_COUNT_METRIC{job=\"$PROMETHEUS_JOB_NAME\",datacenter=~\"$DATACENTER_VARIABLE\"}",
            ),
        )

    var nextPanelId = 1
    handlers.forEach { handler ->
        dashboardBuilder.withPanel(createRpsPanel(handler, panelId = nextPanelId++))
        dashboardBuilder.withPanel(createErrorsPanel(handler, panelId = nextPanelId++))
        dashboardBuilder.withPanel(createLatencyPanel(handler, panelId = nextPanelId++))
    }

    return dashboardBuilder.build()
}

private fun createLabelVariable(name: String, label: String, metricSelector: String): QueryVariableBuilder =
    QueryVariableBuilder(name)
        .label(label)
        .datasource(createPrometheusDataSource())
        .query(StringOrMap.createString("label_values($metricSelector, $name)"))
        .multi(true)
        .includeAll(true)
        .allValue(".*")
        .current(
            VariableOption().apply {
                selected = true
                text = StringOrArrayOfString.createString("All")
                value = StringOrArrayOfString.createString($$"$__all")
            },
        )
        .refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
        .sort(VariableSort.ALPHABETICAL_ASC)
