package de.codecentric

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */

val log: Logger = LoggerFactory.getLogger("de.codecentric.Runner")

fun main(args: Array<String>) {

    log.info("starting up")

    // setup  Kafka config
    val bootstrapServers = "localhost:9092"
    val kafka = Kafka(bootstrapServers)

    val topic = "kt-topic"

    // start a Thread with a Consumer
    val tc = Thread(consumerRunnable(kafka, topic))
    tc.start()

    // give the consumer a little time to start
    Thread.sleep(300)

    // now start the Producer thread
    val tp = Thread(producerRunnable(kafka, topic))
    tp.start()

    tp.join()

    // consumer will be shutdown on program termination (ctrl-c)
    tc.join()
}

private fun consumerRunnable(kafka: Kafka, topic: String) = Runnable {
    val consumer = Consumer(kafka, topic)
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        consumer.stop()
    }))
    consumer.consume {
        log.info("got $it")
    }
}

private fun producerRunnable(kafka: Kafka, topic: String) = Runnable {
    val producer = Producer(kafka, topic)
    (1..10).forEach {
        val msg = "test message $it ${LocalDateTime.now()}"
        log.info("sending $msg")
        producer.send(msg)
    }
    producer.flush()
}

