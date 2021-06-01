package streams.service.sink.strategy

import org.junit.Test
import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.strategy.pattern.NodePatternIngestionStrategy
import org.neo4j.graph_integration.utils.IngestionUtils
import kotlin.test.assertEquals

class NodePatternIngestionStrategyTest {

    @Test
    fun `should get all properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                    "properties" to mapOf("foo" to "foo", "bar" to "bar", "foobar" to "foobar"))
                ),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should get nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo.bar})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"))

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
            queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar"))),
            queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should exclude nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, -foo})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("prop" to 100))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should include nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar", "foo.foobar" to "foobar"))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should exclude the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,-foo,-bar})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foobar" to "foobar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should include the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,foo,bar})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Map<String, Any>>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun `should delete the node`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy<Map<String, Any>, Nothing>(config)
        val data: Map<String, Any> = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity(data, null))
        val queryEvents = strategy.deleteNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MATCH (n:LabelA:LabelB{id: event.keys.id})
                |DETACH DELETE n
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

}