package de.codecentric

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

/**
 * @author P.J. Meisch (peter-josef.meisch@codecentric.de)
 */

class Kafka(val bootstrapServers: String)

class Producer(kafka: Kafka, private val topic: String) {
    private val kafkaProducer: KafkaProducer<String, String>

    init {
        val config = Properties()
        config[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        kafkaProducer = KafkaProducer(config)
    }

    fun send(msg: String) {
        kafkaProducer.send(ProducerRecord(topic, msg))
    }

    fun flush() = kafkaProducer.flush()
}

class Consumer(kafka: Kafka, topic: String) {
    private val kafkaConsumer: KafkaConsumer<String, String>

    @Volatile
    var keepGoing = true

    init {
        val config = Properties()
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
        kafkaConsumer = KafkaConsumer<String, String>(config).apply {
            subscribe(listOf(topic))
        }
    }

    fun consume(onMessage: (value: String) -> Unit) = Thread(Runnable {
        keepGoing = true
        kafkaConsumer.use { kc ->
            while (keepGoing) {
                kc.poll(500)?.forEach {
                    onMessage(it?.value() ?: "???")
                }
            }
        }
    }).start()

    fun stop() {
        keepGoing = false
    }
}
