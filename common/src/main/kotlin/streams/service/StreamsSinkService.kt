package streams.service

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.IngestionStrategy


const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
const val STREAMS_TOPIC_CDC_KEY: String = "streams.sink.topic.cdc"

enum class TopicTypeGroup { CYPHER, CDC, PATTERN, CUD }
enum class TopicType(val group: TopicTypeGroup, val key: String) {
    CDC_SOURCE_ID(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.sourceId"),
    CYPHER(group = TopicTypeGroup.CYPHER, key = "$STREAMS_TOPIC_KEY.cypher"),
    PATTERN_NODE(group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.node"),
    PATTERN_RELATIONSHIP(group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.relationship"),
    CDC_SCHEMA(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.schema"),
    CUD(group = TopicTypeGroup.CUD, key = "$STREAMS_TOPIC_KEY.cud")
}


abstract class StreamsStrategyStorage {
    abstract fun getTopicType(topic: String): TopicType?

    abstract fun getStrategy(topic: String): IngestionStrategy<Any, Any>
}

abstract class StreamsSinkService(private val streamsStrategyStorage: StreamsStrategyStorage) {

    abstract fun write(query: String, events: Collection<Any>)


    private fun writeWithStrategy(data: Collection<Entity<Any, Any>>, strategy: IngestionStrategy<Any, Any>) {
        strategy.mergeNodeEvents(data).events.forEach { write(it.query, it.events) }
        strategy.deleteNodeEvents(data).events.forEach { write(it.query, it.events) }

        strategy.mergeRelationshipEvents(data).events.forEach { write(it.query, it.events) }
        strategy.deleteRelationshipEvents(data).events.forEach { write(it.query, it.events) }
    }

    fun writeForTopic(topic: String, params: Collection<Entity<Any, Any>>) {
        writeWithStrategy(params, streamsStrategyStorage.getStrategy(topic))
    }
}