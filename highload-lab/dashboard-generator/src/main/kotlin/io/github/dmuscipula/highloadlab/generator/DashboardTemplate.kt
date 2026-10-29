package io.github.dmuscipula.highloadlab.generator

private const val DASHBOARD_UID = "highload-lab-generated"
private const val DASHBOARD_TITLE = "Highload lab — generator check"
private const val DASHBOARD_SCHEMA_VERSION = 41
private const val DEFAULT_TIME_RANGE_FROM = "now-15m"
private const val DEFAULT_TIME_RANGE_TO = "now"

private const val HANDLERS_PANEL_ID = 1
private const val HANDLERS_PANEL_TITLE = "Discovered handlers"
private const val GRAFANA_GRID_COLUMNS = 24
private const val HANDLERS_PANEL_HEIGHT = 8

fun createTemplateDashboard(handlers: List<String>): Map<String, Any> = mapOf(
    "uid" to DASHBOARD_UID,
    "title" to DASHBOARD_TITLE,
    "schemaVersion" to DASHBOARD_SCHEMA_VERSION,
    "editable" to false,
    "tags" to listOf("generated"),
    "time" to mapOf("from" to DEFAULT_TIME_RANGE_FROM, "to" to DEFAULT_TIME_RANGE_TO),
    "timezone" to "browser",
    "panels" to listOf(createHandlersPanel(handlers)),
)

private fun createHandlersPanel(handlers: List<String>): Map<String, Any> = mapOf(
    "id" to HANDLERS_PANEL_ID,
    "type" to "text",
    "title" to HANDLERS_PANEL_TITLE,
    "gridPos" to mapOf("x" to 0, "y" to 0, "w" to GRAFANA_GRID_COLUMNS, "h" to HANDLERS_PANEL_HEIGHT),
    "options" to mapOf(
        "mode" to "markdown",
        "content" to formatHandlersMarkdown(handlers),
    ),
)

private fun formatHandlersMarkdown(handlers: List<String>): String = buildString {
    appendLine("## Generator check")
    appendLine("Handlers received from the application:")
    appendLine()
    for (handler in handlers) {
        appendLine("- `$handler`")
    }
}
