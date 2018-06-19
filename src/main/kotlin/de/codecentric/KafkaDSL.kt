package de.codecentric

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */

class KafkaDSL(bootstrapServers: String) {
    private val kafka = Kafka(bootstrapServers)

    fun producer(topic: String, doProduce: Producer.() -> Unit) =
            Producer(kafka, topic).doProduce()

    fun consumer(topic: String, doConsume: Consumer.() -> Unit) =
            Consumer(kafka, topic).doConsume()
}

fun kafka(bootstrapServers: String, init: KafkaDSL.() -> Unit) =
        KafkaDSL(bootstrapServers).init()

