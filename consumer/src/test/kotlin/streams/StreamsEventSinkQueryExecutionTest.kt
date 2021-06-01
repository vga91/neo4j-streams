package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.strategy.cypher.CypherTemplateIngestionStrategy
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.test.rule.DbmsRule
import org.neo4j.test.rule.ImpermanentDbmsRule
import streams.extensions.execute
import streams.kafka.KafkaSinkConfiguration
import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.Topics
import kotlin.test.assertEquals

class StreamsEventSinkQueryExecutionTest {
    private lateinit var db: DbmsRule
    private lateinit var streamsEventSinkQueryExecution: StreamsEventSinkQueryExecution

    @Before
    fun setUp() {
        db = ImpermanentDbmsRule().start()
        val kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(topics = Topics(cypherTopics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
                "    ON CREATE SET n += event.properties"))))
        val streamsTopicService = StreamsTopicService()
        streamsTopicService.set(TopicType.CYPHER, kafkaConfig.streamsSinkConfiguration.topics.cypherTopics)
        streamsEventSinkQueryExecution = StreamsEventSinkQueryExecution(db as GraphDatabaseAPI, NullLog.getInstance(),
                object : StreamsStrategyStorage() {
            override fun getTopicType(topic: String): TopicType? {
                TODO("not implemented")
            }

            override fun getStrategy(topic: String): IngestionStrategy<Any, Any> {
                return CypherTemplateIngestionStrategy(streamsTopicService.getCypherTemplate(topic)!!)
            }

        })
    }

    @After
    fun tearDown() {
        db.shutdownSilently()
    }

    @Test
    fun shouldWriteCypherQuery() {
        // given
        val first = mapOf("id" to "1", "properties" to mapOf("a" to 1))
        val second = mapOf("id" to "2", "properties" to mapOf("a" to 1))

        // when
        streamsEventSinkQueryExecution.writeForTopic("shouldWriteCypherQuery", 
            listOf(Entity(first, first), Entity(second, second)))

        // then
        db.execute("MATCH (n:Label) RETURN count(n) AS count") { it.columnAs<Long>("count").next() }
                .let { assertEquals(2, it) }
    }

}