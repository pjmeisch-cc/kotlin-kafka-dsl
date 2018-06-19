package de.codecentric

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */

class KafkaDSL(bootstrapServers: String) {
    private val kafka = Kafka(bootstrapServers)

    fun producer(topic: String, produce: Producer.() -> Unit) {
        Producer(kafka, topic).produce()
    }

    fun consumer(topic: String, consume: ConsumerDSL.() -> Unit) {
        ConsumerDSL(kafka, topic).consume()
    }
}

fun kafka(bootstrapServers: String, init: KafkaDSL.() -> Unit) {
    KafkaDSL(bootstrapServers).init()
}

class ConsumerDSL(kafka: Kafka, topic: String) {
    private val consumer = Consumer(kafka, topic)

    fun consume(handler: (String) -> Unit) = consumer.consume(handler)

    fun stop() = consumer.stop()
}
