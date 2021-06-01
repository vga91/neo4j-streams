package streams.service.sink.strategy

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.Event
import org.neo4j.graph_integration.IngestionEvent
import org.neo4j.graph_integration.utils.quote
import streams.utils.SchemaUtils.getNodeKeys
import streams.utils.SchemaUtils.toStreamsTransactionEvent
import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.strategy.cud.EntityType
import org.neo4j.graph_integration.utils.IngestionUtils
import org.neo4j.graph_integration.utils.IngestionUtils.getLabelsAsString
import org.neo4j.graph_integration.utils.IngestionUtils.getNodeKeysAsString
import streams.events.Constraint
import streams.events.NodeChange
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.events.StreamsConstraintType
import streams.events.StreamsTransactionEvent


class SchemaIngestionStrategy<KEY, VALUE>: IngestionStrategy<KEY, VALUE>() {

    private fun prepareRelationshipEvents(events: List<StreamsTransactionEvent>, withProperties: Boolean = true): Map<RelationshipSchemaMetadata, List<Map<String, Any>>> = events
            .mapNotNull {
                val payload = it.payload as RelationshipPayload

                val startNodeConstraints = getNodeConstraints(it) {
                    it.type == StreamsConstraintType.UNIQUE && payload.start.labels.orEmpty().contains(it.label)
                }
                val endNodeConstraints = getNodeConstraints(it) {
                    it.type == StreamsConstraintType.UNIQUE && payload.end.labels.orEmpty().contains(it.label)
                }

                if (constraintsAreEmpty(startNodeConstraints, endNodeConstraints)) {
                    null
                } else {
                    createRelationshipMetadata(payload, startNodeConstraints, endNodeConstraints, withProperties)
                }
            }
            .groupBy { it.first }
            .mapValues { it.value.map { it.second } }

    private fun createRelationshipMetadata(payload: RelationshipPayload, 
                                           startNodeConstraints: List<Constraint>, 
                                           endNodeConstraints: List<Constraint>, 
                                           withProperties: Boolean) 
    : Pair<RelationshipSchemaMetadata, Map<String, Map<String, Any>>>? {
        val startNodeKeys = getNodeKeys(
                labels = payload.start.labels.orEmpty(),
                propertyKeys = payload.start.ids.keys,
                constraints = startNodeConstraints)
        val endNodeKeys = getNodeKeys(
                labels = payload.end.labels.orEmpty(),
                propertyKeys = payload.end.ids.keys,
                constraints = endNodeConstraints)
        val start = payload.start.ids.filterKeys { startNodeKeys.contains(it) }
        val end = payload.end.ids.filterKeys { endNodeKeys.contains(it) }

        return if (idsAreEmpty(start, end)) {
            null
        } else {
            val value = if (withProperties) {
                val properties = payload.after?.properties ?: payload.before?.properties ?: emptyMap()
                mapOf("start" to start, "end" to end, "properties" to properties)
            } else {
                mapOf("start" to start, "end" to end)
            }
            val key = RelationshipSchemaMetadata(
                    label = payload.label,
                    startLabels = payload.start.labels.orEmpty().filter { label -> startNodeConstraints.any { it.label == label } },
                    endLabels = payload.end.labels.orEmpty().filter { label -> endNodeConstraints.any { it.label == label } },
                    startKeys = start.keys,
                    endKeys = end.keys
            )
            key to value
        }
    }

    private fun idsAreEmpty(start: Map<String, Any>, end: Map<String, Any>) =
            start.isEmpty() || end.isEmpty()

    private fun constraintsAreEmpty(startNodeConstraints: List<Constraint>, endNodeConstraints: List<Constraint>) =
            startNodeConstraints.isEmpty() || endNodeConstraints.isEmpty()

    override fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>) = prepareRelationshipEvents(events
            .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship
                    && it.meta.operation != OperationType.deleted } })
            .map {
                val label = it.key.label.quote()
                val query = """
                    |${IngestionUtils.UNWIND}
                    |MERGE (start${getLabelsAsString(it.key.startLabels)}{${getNodeKeysAsString("start", it.key.startKeys)}})
                    |MERGE (end${getLabelsAsString(it.key.endLabels)}{${getNodeKeysAsString("end", it.key.endKeys)}})
                    |MERGE (start)-[r:$label]->(end)
                    |SET r = event.properties
                """.trimMargin()
                Event(query, it.value) 
            }.let { IngestionEvent(it) }

    override fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>) = prepareRelationshipEvents(events
            .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship 
                    && it.meta.operation == OperationType.deleted } }, false)
            .map { 
                val label = it.key.label.quote()
                val query = """
                    |${IngestionUtils.UNWIND}
                    |MATCH (start${getLabelsAsString(it.key.startLabels)}{${getNodeKeysAsString("start", it.key.startKeys)}})
                    |MATCH (end${getLabelsAsString(it.key.endLabels)}{${getNodeKeysAsString("end", it.key.endKeys)}})
                    |MATCH (start)-[r:$label]->(end)
                    |DELETE r
                """.trimMargin()
                Event(query, it.value)
            }.let { IngestionEvent(it) }
    

    override fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>) =
        events.mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted } }
            .mapNotNull {
                val changeEvtBefore = it.payload.before as NodeChange
                val constraints = getNodeConstraints(it) { it.type == StreamsConstraintType.UNIQUE }
                if (constraints.isEmpty()) {
                    null
                } else {
                    constraints to mapOf("properties" to changeEvtBefore.properties)
                }
            }
            .groupBy({ it.first }, { it.second })
            .map {
                val labels = it.key.mapNotNull { it.label }
                val nodeKeys = it.key.flatMap { it.properties }.toSet()
                val query = """
                    |${IngestionUtils.UNWIND}
                    |MATCH (n${getLabelsAsString(labels)}{${getNodeKeysAsString(keys = nodeKeys)}})
                    |DETACH DELETE n
                """.trimMargin()
                Event(query, it.value)
            }.let { IngestionEvent(it) }
    

    override fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        val filterLabels: (List<String>, List<Constraint>) -> List<String> = { labels, constraints ->
            labels.filter { label -> !constraints.any { constraint -> constraint.label == label } }
                .map { it.quote() }
        }
        return events
            .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted } }
            .mapNotNull {
                val changeEvtAfter = it.payload.after as NodeChange
                val labelsAfter = changeEvtAfter.labels ?: emptyList()
                val labelsBefore = (it.payload.before as? NodeChange)?.labels.orEmpty()

                val constraints = getNodeConstraints(it) { it.type == StreamsConstraintType.UNIQUE }
                if (constraints.isEmpty()) {
                    null
                } else {
                    val labelsToAdd = filterLabels((labelsAfter - labelsBefore), constraints)
                    val labelsToDelete = filterLabels((labelsBefore - labelsAfter), constraints)

                    val propertyKeys = changeEvtAfter.properties?.keys ?: emptySet()
                    val keys = getNodeKeys(labelsAfter, propertyKeys, constraints)

                    if (keys.isEmpty()) {
                        null
                    } else {
                        val key = NodeSchemaMetadata(
                            constraints = constraints,
                            labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete,
                            keys = keys
                        )
                        val value = mapOf("properties" to changeEvtAfter.properties)
                        key to value
                    }
                }
            }
            .groupBy({ it.first }, { it.second })
            .map { map ->
                var query = """
                        |${IngestionUtils.UNWIND}
                        |MERGE (n${getLabelsAsString(map.key.constraints.mapNotNull { it.label })}{${
                    getNodeKeysAsString(
                        keys = map.key.keys
                    )
                }})
                        |SET n = event.properties
                    """.trimMargin()
                if (map.key.labelsToAdd.isNotEmpty()) {
                    query += "\nSET n${getLabelsAsString(map.key.labelsToAdd)}"
                }
                if (map.key.labelsToDelete.isNotEmpty()) {
                    query += "\nREMOVE n${getLabelsAsString(map.key.labelsToDelete)}"
                }
                Event(query, map.value)
            }.let { IngestionEvent(it) }
    }

    private fun getNodeConstraints(event: StreamsTransactionEvent,
                                   filter: (Constraint) -> Boolean): List<Constraint> = event.schema.constraints.filter { filter(it) }

}