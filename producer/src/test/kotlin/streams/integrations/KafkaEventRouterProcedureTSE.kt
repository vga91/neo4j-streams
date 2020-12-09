package streams.integrations

import org.junit.Test
import org.neo4j.graphdb.QueryExecutionException
import streams.events.StreamsEvent
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.start
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KafkaEventRouterProcedureTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testProcedure() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        db.execute("CALL streams.publish('neo4j', '$message')")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        }}
    }

    @Test
    fun testProcedureWithKey() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        val keyRecord = "test"
        db.execute("CALL streams.publish('neo4j', '$message', {key: '$keyRecord'} )")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
            && JSONUtils.readValue<String>(it.key()).let { keyRecord == it }
        }}
    }

    @Test
    fun testProcedureWithKeyAsMap() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        val keyRecord = mutableMapOf("one" to "Foo", "two" to "Baz", "three" to "Bar")
        db.execute("CALL streams.publish('neo4j', '$message', {key: \$key } )", mapOf("key" to keyRecord))
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
            && JSONUtils.readValue<Map<String, String>>(it.key()).let {
                keyRecord["one"] == it["one"]
                        && keyRecord["two"] == it["two"]
                        && keyRecord["three"] == it["three"]
            }
        }}
    }

    @Test
    fun testProcedureWithPartitionAsNotNumber() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = "notNumber"
        assertFailsWith(QueryExecutionException::class) {
            db.execute("CALL streams.publish('neo4j', '$message', {key: '$keyRecord', partition: '$partitionRecord' })")
        }
    }

    @Test
    fun testProcedureWithPartitionAndKey() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = 0
        db.execute("CALL streams.publish('neo4j', '$message', {key: '$keyRecord', partition: $partitionRecord })")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue{ records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
            && JSONUtils.readValue<String>(it.key()).let { keyRecord == it }
            && JSONUtils.readValue<Int>(it.partition()).let { partitionRecord == it }
        }}
    }

}