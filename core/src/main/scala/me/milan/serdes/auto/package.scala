package me.milan.serdes

import scala.math.BigDecimal

import com.sksamuel.avro4s.{ Decoder, Encoder, FieldMapper, ScalePrecision, SchemaFor }

package object auto {
  implicit val fm: FieldMapper = AvroSerde.fieldMapper
  implicit val sp: ScalePrecision = AvroSerde.scalePrecision
  implicit val rm: BigDecimal.RoundingMode.Value = AvroSerde.roundingMode

  implicit def autoSchema[T >: Null: SchemaFor: Encoder: Decoder]: AvroSerde[T] =
    AvroSerde[T](SchemaFor[T].schema(fm))
}
