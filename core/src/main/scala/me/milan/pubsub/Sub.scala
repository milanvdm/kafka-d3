package me.milan.pubsub

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.Applicative
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.MVar
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

import me.milan.config.KafkaConfig
import me.milan.domain.{ Key, Record, Topic }
import me.milan.pubsub.kafka.KConsumer
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId
import me.milan.serdes.AvroSerde

object Sub {

  def mock[F[_], V](
    stream: Stream[F, Record[V]]
  )(
    implicit
    A: Applicative[F]
  ): Sub[F, V] = MockSub(stream)

  def kafka[F[_], V >: Null: SchemaFor: Decoder: Encoder](
    config: KafkaConfig,
    consumerGroupId: ConsumerGroupId,
    topic: Topic
  )(
    implicit
    C: ConcurrentEffect[F]
  ): F[Sub[F, V]] =
    MVar.empty[F, KafkaConsumer[String, GenericRecord]].flatMap { kafkaConsumer =>
      SignallingRef[F, Boolean](false).flatMap { pauseSignal =>
        SignallingRef[F, Boolean](false).map { haltSignal =>
          new KafkaSub[F, V](config, consumerGroupId, topic, kafkaConsumer, pauseSignal, haltSignal)
        }
      }
    }
}

trait Sub[F[_], V] {

  def start: Stream[F, Record[V]]
  def pause: F[Unit]
  def resume: F[Unit]
  def reset: F[Unit]
  def stop: F[Unit]

}

private[pubsub] class KafkaSub[F[_], V >: Null: SchemaFor: Decoder: Encoder](
  config: KafkaConfig,
  consumerGroupId: ConsumerGroupId,
  topic: Topic,
  kafkaConsumer: MVar[F, KafkaConsumer[String, GenericRecord]],
  pauseSignal: SignallingRef[F, Boolean],
  haltSignal: SignallingRef[F, Boolean]
)(
  implicit
  C: ConcurrentEffect[F]
) extends Sub[F, V] {

  //TODO: Check if stream is stopped (and initialized) before starting
  override def start: Stream[F, Record[V]] = {

    val create = Stream
      .eval(
        haltSignal.set(false).flatMap { _ =>
          kafkaConsumer.tryTake.flatMap { _ =>
            KConsumer(config, consumerGroupId).flatMap(consumer => kafkaConsumer.put(consumer.consumer))
          }
        }
      )
      .drain

    val subscription = Stream
      .eval(subscribe(topic))
      .drain

    val shutdown = Stream
      .eval {
        kafkaConsumer.read
          .map { consumer =>
            consumer.commitAsync()
            consumer.close()
          }
          .recover {
            case _: ConcurrentModificationException => ()
          }
      }

    val poll =
      Stream
        .eval {
          kafkaConsumer.read.map { consumer =>
            val valueAvroSerde = new AvroSerde[V]

            val consumerRecords = consumer
              .poll(500.millis.toJava)
              .records(topic.value)
              .asScala
              .toList

            consumerRecords.map { record =>
              Record(
                Topic(record.topic),
                Key(record.key),
                valueAvroSerde.decode(record.value),
                record.timestamp
              )
            }
          }
        }
        .flatMap(Stream.emits)
        .repeat
        .pauseWhen(pauseSignal)
        .interruptWhen(haltSignal)
        .onFinalize(shutdown.compile.drain)

    create ++ subscription ++ poll
  }

  override def pause: F[Unit] = pauseSignal.set(true).map(_ => ())

  override def resume: F[Unit] = pauseSignal.set(false).map(_ => ())

  //TODO: add logic with adminClient
  override def reset: F[Unit] = kafkaConsumer.read.map { consumer =>
    //val partitions = kafkaConsumer.assignment.asScala.filter(_.topic == topic.value)
    consumer.seekToBeginning(List.empty.asJava) //(partitions.asJava)
  }

  override def stop: F[Unit] = haltSignal.set(true).map(_ => ())

  /**
    * Does only start of being assigned partitions after the first poll
    */
  private def subscribe(topic: Topic): F[Unit] = kafkaConsumer.read.map { consumer =>
    consumer.subscribe(List(topic.value).asJavaCollection)
  }
}

private[pubsub] case class MockSub[F[_], V](
  stream: Stream[F, Record[V]]
)(
  implicit
  A: Applicative[F]
) extends Sub[F, V] {

  override def start: Stream[F, Record[V]] = stream
  override def pause: F[Unit] = A.pure(())
  override def resume: F[Unit] = A.pure(())
  override def reset: F[Unit] = A.pure(())
  override def stop: F[Unit] = A.pure(())

}
