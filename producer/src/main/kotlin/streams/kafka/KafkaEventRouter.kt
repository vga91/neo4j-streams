package streams.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.config.StreamsConfig
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import streams.utils.JSONUtils
import streams.utils.KafkaValidationUtils.getInvalidTopicsError
import streams.utils.StreamsUtils
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom


class KafkaEventRouter: StreamsEventRouter {
    private val log: Log
    private lateinit var producer: Neo4jKafkaProducer<String, ByteArray>
    private lateinit var kafkaConfig: KafkaConfiguration
    private lateinit var kafkaAdminService: KafkaAdminService

    constructor(logService: LogService, config: StreamsConfig, dbName: String): super(logService, config, dbName) {
        log = logService.getUserLog(KafkaEventRouter::class.java)
    }

    override fun printInvalidTopics() {
        val invalidTopics = kafkaAdminService.getInvalidTopics()
        if (invalidTopics.isNotEmpty()) {
            log.warn(getInvalidTopicsError(invalidTopics))
        }
    }

    override fun start() {
        log.info("Initialising Kafka Connector")
        kafkaConfig = KafkaConfiguration.from(config.config)
        val props = kafkaConfig.asProperties()
        val definedTopics = StreamsEventRouterConfiguration
                .from(config, dbName)
                .allTopics()
        kafkaAdminService = KafkaAdminService(kafkaConfig, definedTopics, log)
        kafkaAdminService.start()
        producer = Neo4jKafkaProducer(props)
        producer.initTransactions()
        log.info("Kafka Connector started")
    }

    override fun stop() {
        StreamsUtils.ignoreExceptions({ producer.close() }, Exception::class.java)
        StreamsUtils.ignoreExceptions({ kafkaAdminService.stop() }, Exception::class.java)
    }

    private fun send(producerRecord: ProducerRecord<String, ByteArray>) {
        if (!kafkaAdminService.isValidTopic(producerRecord.topic())) {
            if (log.isDebugEnabled) {
                log.debug("Error while sending record to ${producerRecord.topic()}, because it doesn't exists")
            }
            // TODO add logging system here
            return
        }
        producer.send(producerRecord) { meta, error ->
            if (meta != null && log.isDebugEnabled) {
                log.debug("Successfully sent record in partition ${meta.partition()} offset ${meta.offset()} data ${meta.topic()} key size ${meta.serializedKeySize()}")
            }
            if (error != null) {
                if (log.isDebugEnabled) {
                    log.debug("Error while sending record to ${producerRecord.topic()}, because of the following exception:", error)
                }
            }
        }
    }

    private fun sendEvent(topic: String, event: StreamsEvent, config: Map<String, Any?>) {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a simple event with payload ${event.payload} to kafka")
        }
        val key = config.getOrDefault("key", UUID.randomUUID().toString())

        val producerRecord = ProducerRecord(topic, getPartition(config), System.currentTimeMillis(), JSONUtils.writeValueAsBytes(key!!),
                JSONUtils.writeValueAsBytes(event)) as ProducerRecord<String, ByteArray>
        send(producerRecord)
    }

    private fun sendEvent(topic: String, event: StreamsTransactionEvent, config: Map<String, Any?>) {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a transaction event with txId ${event.meta.txId} and txEventId ${event.meta.txEventId} to kafka")
        }
        val producerRecord = ProducerRecord(topic, getPartition(config), System.currentTimeMillis(), getProducerRecordId(event),
                JSONUtils.writeValueAsBytes(event)) as ProducerRecord<String, ByteArray>
        send(producerRecord)
    }

    override fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>, config: Map<String, Any?>) {
        try {
            producer.beginTransaction()
            transactionEvents.forEach {
                if (it is StreamsTransactionEvent) {
                    sendEvent(topic, it, config)

                } else {
                    sendEvent(topic, it, config)
                }
            }
            producer.commitTransaction()
        } catch (e: ProducerFencedException) {
            log.error("Another producer with the same transactional.id has been started. Stack trace is:", e)
            producer.close()
        } catch (e: OutOfOrderSequenceException) {
            log.error("The broker received an unexpected sequence number from the producer. Stack trace is:", e)
            producer.close()
        } catch (e: AuthorizationException) {
            log.error("Error in authorization. Stack trace is:", e)
            producer.close()
        } catch (e: KafkaException) {
            log.error("Generic kafka error. Stack trace is:", e)
            producer.abortTransaction()
        }
    }

    private fun getProducerRecordId(event: StreamsTransactionEvent) = JSONUtils.writeValueAsBytes("${event.meta.txId + event.meta.txEventId}-${event.meta.txEventId}")

    private fun getPartition(config: Map<String, Any?>) = config.getOrDefault("partition", ThreadLocalRandom.current().nextInt(kafkaConfig.numPartitions)).toString().toInt()

}

class Neo4jKafkaProducer<K, V>: KafkaProducer<K, V> {
    private val isTransactionEnabled: Boolean
    constructor(props: Properties): super(props) {
        isTransactionEnabled = props.containsKey("transactional.id")
    }

    override fun initTransactions() {
        if (isTransactionEnabled) {
            super.initTransactions()
        }
    }

    override fun beginTransaction() {
        if (isTransactionEnabled) {
            super.beginTransaction()
        }
    }

    override fun commitTransaction() {
        if (isTransactionEnabled) {
            super.commitTransaction()
        }
    }

    override fun abortTransaction() {
        if (isTransactionEnabled) {
            super.abortTransaction()
        }
    }
    
}