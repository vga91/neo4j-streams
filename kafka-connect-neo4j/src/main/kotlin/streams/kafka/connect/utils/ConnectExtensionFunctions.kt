package streams.kafka.connect.utils

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.graph_integration.Entity
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.utils.JSONUtils

fun SinkRecord.toEntity(): Entity<Any, Any> = Entity(
        convertData(this.key(),true),
        convertData(this.value()))

private val converter = Neo4jValueConverter()

private fun convertData(data: Any?, stringWhenFailure :Boolean = false) = when (data) {
    is Struct -> converter.convert(data)
    null -> null
    else -> JSONUtils.readValue<Any>(data, stringWhenFailure)
}