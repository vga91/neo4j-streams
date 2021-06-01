package streams

import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.strategy.cud.CUDIngestionStrategy
import org.neo4j.graph_integration.strategy.cypher.CypherTemplateIngestionStrategy
import org.neo4j.graph_integration.strategy.pattern.NodePatternIngestionStrategy
import org.neo4j.graph_integration.strategy.pattern.RelationshipPatternIngestionStrategy
import org.neo4j.graphdb.GraphDatabaseService
import streams.extensions.isDefaultDb
import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.sink.strategy.NodePatternConfiguration
import streams.service.sink.strategy.RelationshipPatternConfiguration
import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy

class Neo4jStreamsStrategyStorage(private val streamsTopicService: StreamsTopicService,
                                  private val streamsConfig: Map<String, String>,
                                  private val db: GraphDatabaseService): StreamsStrategyStorage() {

    override fun getTopicType(topic: String): TopicType? {
        return streamsTopicService.getTopicType(topic)
    }

    private fun <T> getTopicsByTopicType(topicType: TopicType): T = streamsTopicService.getByTopicType(topicType) as T

    override fun getStrategy(topic: String): IngestionStrategy<Any, Any> = when (val topicType = getTopicType(topic)) {
        TopicType.CDC_SOURCE_ID -> {
            val strategyConfig = StreamsSinkConfiguration
                    .createSourceIdIngestionStrategyConfig(streamsConfig, db.databaseName(), db.isDefaultDb())
            SourceIdIngestionStrategy(strategyConfig)
        }
        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
        TopicType.CUD -> CUDIngestionStrategy()
        TopicType.PATTERN_NODE -> {
            val map = getTopicsByTopicType<Map<String, NodePatternConfiguration>>(topicType)
            NodePatternIngestionStrategy(map.getValue(topic))
        }
        TopicType.PATTERN_RELATIONSHIP -> {
            val map = getTopicsByTopicType<Map<String, RelationshipPatternConfiguration>>(topicType)
            RelationshipPatternIngestionStrategy(map.getValue(topic))
        }
        TopicType.CYPHER -> {
            CypherTemplateIngestionStrategy(streamsTopicService.getCypherTemplate(topic)!!)
        }
        else -> throw RuntimeException("Topic Type not Found")
    }

}