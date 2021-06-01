package streams.service.sink.strategy

import org.junit.Test
import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.utils.IngestionUtils
import streams.events.*
//import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils
import kotlin.test.assertEquals

class SourceIdIngestionStrategyTest {

    @Test
    fun `should create the Merge Query Strategy for mixed events`() {
        // given
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val config = SourceIdIngestionStrategyConfig(labelName = "Custom SourceEvent", idName = "custom Id")
        val cdcQueryStrategy = SourceIdIngestionStrategy<StreamsTransactionEvent, StreamsTransactionEvent>(config)
        val txEvents = listOf(Entity(cdcDataStart, cdcDataStart),
                Entity(cdcDataEnd, cdcDataEnd),
                Entity(cdcDataRelationship, cdcDataRelationship))

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents).events
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents).events

        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents).events
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents).events

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |${IngestionUtils.UNWIND}
            |MERGE (n:`Custom SourceEvent`{`custom Id`: event.id})
            |SET n = event.properties
            |SET n.`custom Id` = event.id
            |SET n:User
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0", "properties" to mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA")),
                mapOf("id" to "1", "properties" to mapOf("name" to "Michael", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)

        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |${IngestionUtils.UNWIND}
            |MERGE (start:`Custom SourceEvent`{`custom Id`: event.start})
            |MERGE (end:`Custom SourceEvent`{`custom Id`: event.end})
            |MERGE (start)-[r:`KNOWS WHO`{`custom Id`: event.id}]->(end)
            |SET r = event.properties
            |SET r.`custom Id` = event.id
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("id" to "2", "start" to "0", "end" to "1", "properties" to mapOf("since" to 2014))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Merge Query Strategy for node updates`() {
        // given
        val nodeSchema = Schema()
        // given
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.updated
                ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.updated
                ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = SourceIdIngestionStrategy<StreamsTransactionEvent, StreamsTransactionEvent>()
        val txEvents = listOf(Entity(cdcDataStart, cdcDataStart), Entity(cdcDataEnd, cdcDataEnd))

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents).events
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents).events

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |${IngestionUtils.UNWIND}
            |MERGE (n:SourceEvent{sourceId: event.id})
            |SET n = event.properties
            |SET n.sourceId = event.id
            |REMOVE n:ToRemove
            |SET n:NewLabel
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0", "properties" to mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA")),
                mapOf("id" to "1", "properties" to mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Merge Query Strategy for relationships updates`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.updated
                ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "label")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = SourceIdIngestionStrategy<StreamsTransactionEvent, StreamsTransactionEvent>()
        val txEvents = listOf(Entity(cdcDataRelationship, cdcDataRelationship))

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents).events
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents).events

        // then
        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |${IngestionUtils.UNWIND}
            |MERGE (start:SourceEvent{sourceId: event.start})
            |MERGE (end:SourceEvent{sourceId: event.end})
            |MERGE (start)-[r:`KNOWS WHO`{sourceId: event.id}]->(end)
            |SET r = event.properties
            |SET r.sourceId = event.id
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("id" to "2", "start" to "0", "end" to "1", "properties" to mapOf("since" to 2014, "foo" to "label"))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Merge Query Strategy for node deletes`() {
        // given
        val nodeSchema = Schema()
        // given
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.deleted
                ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.deleted
                ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = SourceIdIngestionStrategy<StreamsTransactionEvent, StreamsTransactionEvent>()
        val txEvents = listOf(Entity(cdcDataStart, cdcDataStart), Entity(cdcDataEnd, cdcDataEnd))

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents).events
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents).events

        // then
        assertEquals(1, nodeDeleteEvents.size)
        assertEquals(0, nodeEvents.size)
        val nodeQuery = nodeDeleteEvents[0].query
        val expectedNodeQuery = """
            |${IngestionUtils.UNWIND} MATCH (n:SourceEvent{sourceId: event.id}) DETACH DELETE n
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeDeleteEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0"),
                mapOf("id" to "1")
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Merge Query Strategy for relationships deletes`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.deleted
                ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "label")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = SourceIdIngestionStrategy<StreamsTransactionEvent, StreamsTransactionEvent>()
        val txEvents = listOf(Entity(cdcDataRelationship, cdcDataRelationship))

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents).events
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents).events

        // then
        assertEquals(1, relationshipDeleteEvents.size)
        assertEquals(0, relationshipEvents.size)
        val relQuery = relationshipDeleteEvents[0].query
        val expectedRelQuery = """
            |${IngestionUtils.UNWIND} MATCH ()-[r:`KNOWS WHO`{sourceId: event.id}]-() DELETE r
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipDeleteEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(mapOf("id" to "2"))
        assertEquals(expectedRelEvents, eventsRelList)
    }

}

