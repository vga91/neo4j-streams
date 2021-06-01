package streams.kafka.connect.sink

import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.graph_integration.Entity
import streams.kafka.connect.utils.toEntity

class EventBuilder {
    private var batchSize: Int? = null
    private lateinit var sinkRecords: Collection<SinkRecord>

    fun withBatchSize(batchSize: Int): EventBuilder {
        this.batchSize = batchSize
        return this
    }

    fun withSinkRecords(sinkRecords: Collection<SinkRecord>): EventBuilder {
        this.sinkRecords = sinkRecords
        return this
    }

    fun build(): Map<String, List<List<Entity<Any, Any>>>> {
        val batchSize = this.batchSize!!
        return this.sinkRecords
                .groupBy { it.topic() }
                .mapValues { entry ->
                    val value = entry.value.map { it.toEntity() }
                    if (batchSize > value.size) listOf(value) else value.chunked(batchSize)
                }
    }

}