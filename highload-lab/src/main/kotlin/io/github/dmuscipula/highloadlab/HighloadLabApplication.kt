package io.github.dmuscipula.highloadlab

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class HighloadLabApplication

fun main(args: Array<String>) {
	runApplication<HighloadLabApplication>(*args)
}
