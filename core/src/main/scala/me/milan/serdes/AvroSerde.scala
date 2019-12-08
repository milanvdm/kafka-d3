package me.milan.serdes

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import me.milan.domain.TombStone

trait AvroSerde[T] {
  def encode(record: T): GenericRecord
  def decode(avroRecord: GenericRecord): T
}

object AvroSerde {
  implicit val fieldMapper: FieldMapper = DefaultFieldMapper
  implicit val scalePrecision: ScalePrecision = ScalePrecision(4, 8)
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  def apply[T >: Null: Encoder: Decoder](schema: Schema): AvroSerde[T] = new AvroSerde[T] {

    private val format = RecordFormat[T](schema)

    override def encode(record: T): GenericRecord = record match {
      case _: TombStone => null
      case _            => format.to(record)
    }

    override def decode(avroRecord: GenericRecord): T =
      if (avroRecord == null) null else format.from(avroRecord)
  }

  def apply[T >: Null](implicit serde: AvroSerde[T]): AvroSerde[T] = serde
}
