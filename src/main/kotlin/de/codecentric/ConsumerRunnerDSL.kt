package de.codecentric

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */


fun main(args: Array<String>) {

    val log: Logger = LoggerFactory.getLogger("de.codecentric.ConsumerRunnerDSL")

    log.info("starting up")

    kafka("localhost:9092") {
        consumer("kt-topic") {
            Runtime.getRuntime().addShutdownHook(Thread(Runnable {
                stop()
            }))
            consume {
                log.info("got $it")
            }
        }
    }
}

