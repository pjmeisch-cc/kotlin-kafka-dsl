package de.codecentric

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */


fun main(args: Array<String>) {

    val log: Logger = LoggerFactory.getLogger("de.codecentric.ConsumerRunner")

    log.info("starting up")

    val kafka = Kafka("localhost:9092")
    val topic = "kt-topic"

    val consumer = Consumer(kafka, topic)
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        consumer.stop()
    }))
    consumer.consume {
        log.info("got $it")
    }
}

