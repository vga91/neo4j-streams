package streams.extensions

import org.apache.avro.Schema
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.graph_integration.Entity
import org.neo4j.graphdb.Node
import streams.utils.JSONUtils
import java.nio.ByteBuffer
import java.util.Properties

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue
fun Map<*, *>.asProperties() = this.let {
    val properties = Properties()
    properties.putAll(it)
    properties
}

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}

fun String.toPointCase(): String {
    return this.split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase()
}


fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(this.topic(), this.partition())
fun ConsumerRecord<*, *>.offsetAndMetadata(metadata: String = "") = OffsetAndMetadata(this.offset() + 1, metadata)

private fun convertAvroData(rawValue: Any?): Any? = when (rawValue) {
    is IndexedRecord -> rawValue.toMap()
    is Collection<*> -> rawValue.map(::convertAvroData)
    is Array<*> -> if (rawValue.javaClass.componentType.isPrimitive) rawValue else rawValue.map(::convertAvroData)
    is Map<*, *> -> rawValue
            .mapKeys { it.key.toString() }
            .mapValues { convertAvroData(it.value) }
    is GenericFixed -> rawValue.bytes()
    is ByteBuffer -> rawValue.array()
    is GenericEnumSymbol, is CharSequence -> rawValue.toString()
    else -> rawValue
}
fun IndexedRecord.toMap() = this.schema.fields
        .map { it.name() to convertAvroData(this[it.pos()]) }
        .toMap()

fun Schema.toMap() = JSONUtils.asMap(this.toString())

private fun convertData(data: Any?, stringWhenFailure: Boolean = false): Any? {
    return when (data) {
        null -> null
        is ByteArray -> JSONUtils.readValue<Any>(data, stringWhenFailure)
        is GenericRecord -> data.toMap()
        else -> if (stringWhenFailure) data.toString() else throw RuntimeException("Unsupported type ${data::class.java.name}")
    }
}

// TODO - TIPIZZAZIONE, FORSE VA BENE ANY,ANY
fun ConsumerRecord<*, *>.toEntity(): Entity<Any, Any> {
    val key = convertData(this.key(), true)
    val value = convertData(this.value())
    return Entity(key, value)
}