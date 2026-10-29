package io.github.dmuscipula.highloadlab.generator

fun main(args: Array<String>) {
    val config = readConfig(args)
    generateDashboard(config)
}
