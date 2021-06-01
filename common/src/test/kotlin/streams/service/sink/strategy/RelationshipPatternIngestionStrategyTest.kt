package streams.service.sink.strategy

import org.junit.Test
import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.strategy.pattern.RelationshipPatternIngestionStrategy
import org.neo4j.graph_integration.utils.IngestionUtils
import kotlin.test.assertEquals

class RelationshipPatternIngestionStrategyTest {

    @Test
    fun `should get all properties`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${IngestionUtils.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
    }

    @Test
    fun `should get all properties - simple`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "$startPattern REL_TYPE $endPattern"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${IngestionUtils.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
    }

    @Test
    fun `should get all properties with reverse start-end`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$endPattern)<-[:REL_TYPE]-(:$startPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${IngestionUtils.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
    }

    @Test
    fun `should get nested properties`() {
        // given
        val startPattern = "LabelA{!idStart, foo.mapFoo}"
        val endPattern = "LabelB{!idEnd, bar.mapBar}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to mapOf("mapFoo" to "mapFoo"),
                "bar" to mapOf("mapBar" to "mapBar"),
                "rel" to 1,
                "map" to mapOf("a" to "a", "inner" to mapOf("b" to "b")))

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${IngestionUtils.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(
                mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to mapOf("foo.mapFoo" to "mapFoo")),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to mapOf("bar.mapBar" to "mapBar")),
                "properties" to mapOf("rel" to 1, "map.a" to "a", "map.inner.b" to "b"))
        ), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
    }

    @Test
    fun `should delete the relationship`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy<Map<String, Any>, Nothing>(config)
        val data: Map<String, Any> = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(Entity(data, null))
        val queryEvents = strategy.deleteRelationshipEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${IngestionUtils.UNWIND}
            |MATCH (start:LabelA{idStart: event.start.keys.idStart})
            |MATCH (end:LabelB{idEnd: event.end.keys.idEnd})
            |MATCH (start)-[r:REL_TYPE]->(end)
            |DELETE r
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
    }

}