package streams.kafka.connect.sink

import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.strategy.cud.CUDIngestionStrategy
import org.neo4j.graph_integration.strategy.cypher.CypherTemplateIngestionStrategy
import org.neo4j.graph_integration.strategy.pattern.NodePatternIngestionStrategy
import org.neo4j.graph_integration.strategy.pattern.RelationshipPatternIngestionStrategy
import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.sink.strategy.SchemaIngestionStrategy

class Neo4jStrategyStorage(val config: Neo4jSinkConnectorConfig): StreamsStrategyStorage() {
    private val topicConfigMap = config.topics.asMap()

    override fun getTopicType(topic: String): TopicType? = TopicType.values().firstOrNull { topicType ->
        when (val topicConfig = topicConfigMap.getOrDefault(topicType, emptyList<Any>())) {
            is Collection<*> -> topicConfig.contains(topic)
            is Map<*, *> -> topicConfig.containsKey(topic)
            else -> false
        }
    }

    override fun getStrategy(topic: String) : IngestionStrategy<Any, Any> = when (val topicType = getTopicType(topic)) {
        TopicType.CDC_SOURCE_ID -> config.strategyMap[topicType] as IngestionStrategy<Any, Any>
        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
        TopicType.CUD -> CUDIngestionStrategy()
        TopicType.PATTERN_NODE -> NodePatternIngestionStrategy(config.topics.nodePatternTopics.getValue(topic))
        TopicType.PATTERN_RELATIONSHIP -> RelationshipPatternIngestionStrategy(config.topics.relPatternTopics.getValue(topic))
        TopicType.CYPHER -> CypherTemplateIngestionStrategy(config.topics.cypherTopics.getValue(topic))
        null -> throw RuntimeException("Topic Type not Found")
    }
}