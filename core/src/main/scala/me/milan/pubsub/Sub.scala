package me.milan.pubsub

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.Applicative
import cats.effect.ConcurrentEffect
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.http4s.Uri.Host

import me.milan.config.KafkaConfig
import me.milan.domain.{ Done, Key, Record, Topic }
import me.milan.pubsub.kafka.KConsumer
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
    topic: Topic
  )(
    implicit
    C: ConcurrentEffect[F]
  ): F[Sub[F, V]] =
    SignallingRef[F, Boolean](false).flatMap { pauseSignal ⇒
      SignallingRef[F, Boolean](false).map { haltSignal ⇒
        KafkaSub[F, V](config, topic, pauseSignal, haltSignal)
      }
    }
}

trait Sub[F[_], V] {

  def start: Stream[F, Record[V]]
  def pause: F[Done]
  def unPause: F[Done]
  def hosts: F[Set[Host]]
  def reset: F[Done]
  def stop: F[Done]

}

private[pubsub] case class KafkaSub[F[_], V >: Null: SchemaFor: Decoder: Encoder](
  config: KafkaConfig,
  topic: Topic,
  pauseSignal: SignallingRef[F, Boolean],
  haltSignal: SignallingRef[F, Boolean]
)(
  implicit
  C: ConcurrentEffect[F]
) extends Sub[F, V] {

  private val kafkaConsumer: KafkaConsumer[String, GenericRecord] = new KConsumer(config).consumer

  override def start: Stream[F, Record[V]] = {

    val subscription = Stream
      .eval(subscribe(topic))
      .drain

    val shutdown = Stream
      .eval {
        C.delay {
            //TODO: Add callback on commit
            kafkaConsumer.commitAsync()
            kafkaConsumer.close()
          }
          .recover {
            case _: ConcurrentModificationException ⇒ ()
          }
      }

    val poll =
      Stream
        .eval {
          C.delay {
            val valueAvroSerde = new AvroSerde[V]

            val consumerRecords = kafkaConsumer
              .poll(500.millis.toJava)
              .records(topic.value)
              .asScala
              .toList

            consumerRecords.map { record ⇒
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

    subscription ++ poll
  }

  override def pause: F[Done] = pauseSignal.set(true).map(_ ⇒ Done)

  override def unPause: F[Done] = pauseSignal.set(false).map(_ ⇒ Done)

  override def hosts: F[Set[Host]] =
    ???

  override def reset: F[Done] =
    C.delay {
      val partitions = kafkaConsumer.assignment().asScala.toList.filter(_.topic == topic.value)
      kafkaConsumer.seekToBeginning(partitions.asJava)
      Done
    }

  //TODO: Check if stopped, it also gets removed from consumer-group or not
  override def stop: F[Done] = haltSignal.set(true).map(_ ⇒ Done)

  /**
    * Does only start of being assigned partitions after the first poll
    */
  private def subscribe(topic: Topic): F[Done] = C.delay {
    kafkaConsumer.subscribe(List(topic).map(_.value).asJavaCollection)
    Done
  }
}

private[pubsub] case class MockSub[F[_], V](
  stream: Stream[F, Record[V]]
)(
  implicit
  A: Applicative[F]
) extends Sub[F, V] {

  override def start: Stream[F, Record[V]] = stream
  override def pause: F[Done] = A.pure(Done)
  override def unPause: F[Done] = A.pure(Done)
  override def hosts: F[Set[Host]] = A.pure(Set.empty)
  override def reset: F[Done] = A.pure(Done)
  override def stop: F[Done] = A.pure(Done)

}
