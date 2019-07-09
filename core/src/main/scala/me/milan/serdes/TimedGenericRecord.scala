package me.milan.serdes

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serdes, Serializer }

case class TimedGenericRecord(
  record: GenericRecord,
  timestamp: Long
)

object TimedGenericRecord {
  class TimeGenericRecordSerializer(
    genericRecordSerializer: Serializer[GenericRecord],
    longSerializer: Serializer[Long]
  ) extends Serializer[TimedGenericRecord] {

    override def configure(
      configs: java.util.Map[String, _],
      isKey: Boolean
    ): Unit = ()

    override def serialize(
      topic: String,
      data: TimedGenericRecord
    ): Array[Byte] =
      genericRecordSerializer.serialize(topic, data.record) ++
          longSerializer.serialize(topic, data.timestamp)

    override def close(): Unit = {
      genericRecordSerializer.close()
      longSerializer.close()
    }
  }

  class TimeGenericRecordDeserializer(
    genericRecordDeserializer: Deserializer[GenericRecord],
    longDeserializer: Deserializer[Long]
  ) extends Deserializer[TimedGenericRecord] {

    private val LongBytes = 8

    override def configure(
      configs: java.util.Map[String, _],
      isKey: Boolean
    ): Unit = ()

    override def deserialize(
      topic: String,
      bytes: Array[Byte]
    ): TimedGenericRecord = {
      val genericRecord = genericRecordDeserializer.deserialize(topic, bytes.take(bytes.length - LongBytes))
      val timestamp = longDeserializer.deserialize(topic, bytes.takeRight(LongBytes))

      TimedGenericRecord(genericRecord, timestamp)
    }

    override def close(): Unit = {
      genericRecordDeserializer.close()
      longDeserializer.close()
    }
  }

  def serdes(
    genericRecordSerde: Serde[GenericRecord],
    longSerde: Serde[Long]
  ): Serde[TimedGenericRecord] =
    Serdes.serdeFrom(
      new TimeGenericRecordSerializer(genericRecordSerde.serializer, longSerde.serializer),
      new TimeGenericRecordDeserializer(genericRecordSerde.deserializer, longSerde.deserializer)
    )

}
