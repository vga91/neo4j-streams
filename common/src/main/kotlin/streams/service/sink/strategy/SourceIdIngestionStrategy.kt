package streams.service.sink.strategy

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.Event
import org.neo4j.graph_integration.IngestionEvent
import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.strategy.cud.EntityType
import org.neo4j.graph_integration.utils.IngestionUtils
import org.neo4j.graph_integration.utils.IngestionUtils.getLabelsAsString
import org.neo4j.graph_integration.utils.quote
import streams.events.NodeChange
import streams.events.OperationType
import streams.events.RelationshipChange
import streams.events.RelationshipPayload
import streams.utils.SchemaUtils

data class SourceIdIngestionStrategyConfig(val labelName: String = "SourceEvent", val idName: String = "sourceId")

class SourceIdIngestionStrategy<KEY, VALUE>(config: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()): IngestionStrategy<KEY, VALUE>() {

    private val quotedLabelName = config.labelName.quote()
    private val quotedIdName = config.idName.quote()

    override fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>) = events
        .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship && it.meta.operation != OperationType.deleted } }
        .map { data ->
            val payload = data.payload as RelationshipPayload
            val changeEvt = when (data.meta.operation) {
                OperationType.deleted -> {
                    data.payload.before as RelationshipChange
                }
                else -> data.payload.after as RelationshipChange
            }
            payload.label to mapOf("id" to payload.id,
                    "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties)
        }
        .groupBy({ it.first }, { it.second })
        .map {
            val query = """
                |${IngestionUtils.UNWIND}
                |MERGE (start:$quotedLabelName{$quotedIdName: event.start})
                |MERGE (end:$quotedLabelName{$quotedIdName: event.end})
                |MERGE (start)-[r:${it.key.quote()}{$quotedIdName: event.id}]->(end)
                |SET r = event.properties
                |SET r.$quotedIdName = event.id
            """.trimMargin()
            Event(query, it.value)
        }.let { IngestionEvent(it) }
    

    override fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>) = events
        .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship && it.meta.operation == OperationType.deleted } }
        .map { data ->
            val payload = data.payload as RelationshipPayload
            payload.label to mapOf("id" to data.payload.id)
        }
        .groupBy({ it.first }, { it.second })
        .map {
            val query = "${IngestionUtils.UNWIND} MATCH ()-[r:${it.key.quote()}{$quotedIdName: event.id}]-() DELETE r"
            Event(query, it.value)
        }.let { IngestionEvent(it) }
    

    override fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        val data = events
                .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted } }
                .map { mapOf("id" to it.payload.id) }
        return data.let {
            val list = if (it.isEmpty()) {
                emptyList()
            } else {
                val query = "${IngestionUtils.UNWIND} MATCH (n:$quotedLabelName{$quotedIdName: event.id}) DETACH DELETE n"
                listOf(Event(query, data))
            }
            IngestionEvent(list)
        }
    }

    override fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>) = events
        .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted } }
        .map { data ->
            val changeEvtAfter = data.payload.after as NodeChange
            val labelsAfter = changeEvtAfter.labels ?: emptyList()
            val labelsBefore = if (data.payload.before != null) {
                val changeEvtBefore = data.payload.before as NodeChange
                changeEvtBefore.labels ?: emptyList()
            } else {
                emptyList()
            }
            val labelsToAdd = (labelsAfter - labelsBefore)
                    .toSet()
            val labelsToDelete = (labelsBefore - labelsAfter)
                    .toSet()
            NodeMergeMetadata(labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete) to mapOf("id" to data.payload.id, "properties" to changeEvtAfter.properties)
        }
        .groupBy({ it.first }, { it.second })
        .map {
            var query = """
                |${IngestionUtils.UNWIND}
                |MERGE (n:$quotedLabelName{$quotedIdName: event.id})
                |SET n = event.properties
                |SET n.$quotedIdName = event.id
            """.trimMargin()
            if (it.key.labelsToDelete.isNotEmpty()) {
                query += "\nREMOVE n${getLabelsAsString(it.key.labelsToDelete)}"
            }
            if (it.key.labelsToAdd.isNotEmpty()) {
                query += "\nSET n${getLabelsAsString(it.key.labelsToAdd)}"
            }
            Event(query, it.value)
        }.let { IngestionEvent(it) }
    

}