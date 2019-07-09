package me.milan.pubsub

import cats.Applicative
import cats.effect.ConcurrentEffect
import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{ Callback, KafkaProducer, ProducerRecord, RecordMetadata }

import me.milan.domain.Record
import me.milan.serdes.AvroSerde

object Pub {

  def mock[F[_], V](
    implicit
    A: Applicative[F]
  ): Pub[F, V] = MockPub[F, V]

  def kafka[F[_]: ConcurrentEffect, V >: Null: SchemaFor: Decoder: Encoder](
    implicit
    kafkaProducer: KafkaProducer[String, GenericRecord]
  ): Pub[F, V] = new KafkaPub[F, V]

}

trait Pub[F[_], V] {

  def publish(record: Record[V]): F[Unit]

}

private[pubsub] class KafkaPub[F[_]: ConcurrentEffect, V >: Null: SchemaFor: Decoder: Encoder](
  implicit
  kafkaProducer: KafkaProducer[String, GenericRecord]
) extends Pub[F, V] {

  override def publish(record: Record[V]): F[Unit] =
    ConcurrentEffect[F].async { cb =>
      val valueAvroSerde = new AvroSerde[V]

      kafkaProducer
        .send(
          new ProducerRecord(
            record.topic.value,
            record.partitionId.orNull,
            record.timestamp,
            record.key.value,
            valueAvroSerde.encode(record.value)
          ),
          callback {
            case (_, throwable) =>
              cb(Option(throwable).toLeft(()))
          }
        )
      ()
    }

  private def callback(f: (RecordMetadata, Throwable) => Unit): Callback =
    new Callback {
      override def onCompletion(
        metadata: RecordMetadata,
        exception: Exception
      ): Unit =
        f(metadata, exception)
    }
}

private[pubsub] case class MockPub[F[_], V](
)(
  implicit
  A: Applicative[F]
) extends Pub[F, V] {

  override def publish(record: Record[V]): F[Unit] =
    A.pure(())

}
