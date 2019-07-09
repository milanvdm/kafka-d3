package me.milan.serdes

import com.sksamuel.avro4s.{ Decoder, Encoder, RecordFormat, SchemaFor }
import org.apache.avro.generic.GenericRecord

import me.milan.domain.TombStone

class AvroSerde[T >: Null: SchemaFor: Decoder: Encoder] {

  private val format = RecordFormat[T]

  def encode(record: T): GenericRecord = record match {
    case _: TombStone => null
    case _            => format.to(record)
  }

  def decode(avroRecord: GenericRecord): T =
    if (avroRecord == null) null else format.from(avroRecord)

}
