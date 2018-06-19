package de.codecentric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */


fun main(args: Array<String>) {

    val log: Logger = LoggerFactory.getLogger("de.codecentric.ProducerRunnerDSL")

    kafka("localhost:9092") {
        producer("kt-topic") {
            (1..10).forEach {
                val msg = "test message $it ${LocalDateTime.now()}"
                log.info("sending $msg")
                send(msg)
            }
            flush()
        }
    }
}
