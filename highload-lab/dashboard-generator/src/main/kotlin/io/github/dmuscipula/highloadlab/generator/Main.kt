package io.github.dmuscipula.highloadlab.generator

import io.github.dmuscipula.highloadlab.generator.config.readConfig
import io.github.dmuscipula.highloadlab.generator.dashboard.generateDashboard

fun main(args: Array<String>) {
    val config = readConfig(args)
    generateDashboard(config)
}
