package me.milan.serdes

import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }

package object auto {
  implicit def autoSchema[T >: Null: SchemaFor: Encoder: Decoder]: AvroSerde[T] =
    AvroSerde[T](SchemaFor[T].schema)
}
