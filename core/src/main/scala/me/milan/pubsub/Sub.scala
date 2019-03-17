package me.milan.pubsub

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.Applicative
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.functor._
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.http4s.Uri.Host

import me.milan.config.KafkaConfig
import me.milan.domain.{Done, Key, Record, Topic}
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
  ): Sub[F, V] = KafkaSub[F, V](config, topic)

}

trait Sub[F[_], V] {

  def start: Stream[F, Record[V]]
  def pause: F[Done]
  def hosts: F[Set[Host]]
  def reset: F[Done]
  def stop: F[Done]

}

private[pubsub] case class KafkaSub[F[_], V >: Null: SchemaFor: Decoder: Encoder](
  config: KafkaConfig,
  topic: Topic
)(
  implicit
  C: ConcurrentEffect[F]
) extends Sub[F, V] {

  private val kafkaConsumer: KafkaConsumer[String, GenericRecord] = new KConsumer(config).consumer

  private val pauseSignal = SignallingRef[F, Boolean](false)
  private val pauseStream = Stream.eval(pauseSignal)

  private val halt = Deferred[F, Either[Throwable, Unit]]
  private val haltStream = Stream.eval(halt)

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
      pauseStream.flatMap { p =>
        haltStream.flatMap { d ⇒
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
            .pauseWhen(p)
            .interruptWhen(d)
            .onFinalize(shutdown.compile.drain)
        }
      }

    subscription ++ poll
  }

  override def pause: F[Done] =
    pauseSignal.map { p =>
      p.set(true)
      Done
    }

  override def hosts: F[Set[Host]] =
    ???

  override def reset: F[Done] =
    C.delay {
      val partitions = kafkaConsumer.assignment().asScala.toList.filter(_.topic == topic.value)
      kafkaConsumer.seekToBeginning(partitions.asJava)
      Done
    }

  //TODO: Check if stopped, it also gets removed from consumer-group or not
  override def stop: F[Done] = halt.map { d ⇒
    d.complete(().asRight)
    Done
  }

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
  override def hosts: F[Set[Host]] = A.pure(Set.empty)
  override def reset: F[Done] = A.pure(Done)
  override def stop: F[Done] = A.pure(Done)

}
