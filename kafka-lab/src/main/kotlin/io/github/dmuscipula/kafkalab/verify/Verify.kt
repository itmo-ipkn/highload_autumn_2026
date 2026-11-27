package io.github.dmuscipula.kafkalab.verify

object Verify {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.isEmpty()) { "Usage: ./verify" }

        val sink = readSinkSnapshot()
        val deadLetters = readDeadLetterSnapshot()
        val sequencesByUser = (sink.events.map { it.event } + deadLetters)
            .groupBy(EventSequence::userId, EventSequence::seq)
            .toSortedMap()
        val gapsByUser = sequencesByUser.mapValues { (_, sequences) -> findGaps(sequences) }.filterValues { it.isNotEmpty() }
        val lateWritesByUser = sink.events.groupBy { it.event.userId }.toSortedMap()
            .mapValues { (_, events) -> countLateWrites(events) }.filterValues { it > 0 }

        println("Users checked: ${sequencesByUser.size}")
        println("Users with seq gaps: ${gapsByUser.size}")
        for ((userId, gaps) in gapsByUser) {
            val ranges = gaps.joinToString { if (it.first == it.last) "${it.first}" else "${it.first}-${it.last}" }
            println("  $userId: $ranges")
        }
        println("Discarded duplicates: ${sink.discardedDuplicates}")
        println("Out-of-order writes: ${lateWritesByUser.values.sum()}")
        for ((userId, count) in lateWritesByUser) {
            println("  $userId: $count")
        }
        println("Rows without write_order: ${sink.events.count { it.writeOrder == null }}")
    }
}

private fun findGaps(sequences: List<Long>): List<LongRange> {
    val gaps = mutableListOf<LongRange>()
    var previous = 0L
    for (seq in sequences.toSortedSet()) {
        if (seq - previous > 1) gaps.add((previous + 1)..<seq)
        previous = seq
    }
    return gaps
}

private fun countLateWrites(events: List<StoredEvent>): Int {
    var highestSeq = 0L
    var lateWrites = 0
    for ((event, writeOrder) in events) {
        if (writeOrder == null) continue
        if (event.seq < highestSeq) lateWrites++
        highestSeq = maxOf(highestSeq, event.seq)
    }
    return lateWrites
}
