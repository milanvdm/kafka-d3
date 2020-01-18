package me.milan.pubsub

import java.util.ConcurrentModificationException

import scala.jdk.CollectionConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.ConcurrentEffect
import cats.effect.concurrent.MVar
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

import me.milan.config.KafkaConfig
import me.milan.discovery.Registry
import me.milan.discovery.Registry.RegistryGroup
import me.milan.domain.{ Key, Record, Topic }
import me.milan.pubsub.kafka.KConsumer
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId
import me.milan.serdes.AvroSerde

object Sub {

  def kafka[F[_]: ConcurrentEffect, V >: Null: AvroSerde](
    config: KafkaConfig,
    consumerGroupId: ConsumerGroupId,
    topic: Topic,
    discoveryRegistry: Registry[F]
  ): F[Sub[F, V]] =
    MVar.empty[F, KafkaConsumer[String, GenericRecord]].flatMap { kafkaConsumer =>
      SignallingRef[F, Boolean](false).flatMap { pauseSignal =>
        SignallingRef[F, Boolean](false).map { haltSignal =>
          new KafkaSub[F, V](config, consumerGroupId, topic, kafkaConsumer, discoveryRegistry, pauseSignal, haltSignal)
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

private[pubsub] class KafkaSub[F[_]: ConcurrentEffect, V >: Null: AvroSerde](
  config: KafkaConfig,
  consumerGroupId: ConsumerGroupId,
  topic: Topic,
  kafkaConsumer: MVar[F, KafkaConsumer[String, GenericRecord]],
  discoveryRegistry: Registry[F],
  pauseSignal: SignallingRef[F, Boolean],
  haltSignal: SignallingRef[F, Boolean]
) extends Sub[F, V] {

  override def start: Stream[F, Record[V]] = {

    val discoveryRegistration = discoveryRegistry
      .register(RegistryGroup(s"${topic.value}-sub"))
      .interruptWhen(haltSignal)
      .drain

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
            val consumerRecords = consumer
              .poll(500.millis.toJava)
              .records(topic.value)
              .asScala
              .toList

            consumerRecords.map { record =>
              Record(
                Topic(record.topic),
                Key(record.key),
                AvroSerde[V].decode(record.value),
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

    for {
      paused <- Stream.eval(pauseSignal.get)
      halted <- Stream.eval(haltSignal.get)
      records <- if (halted && !paused) discoveryRegistration.merge(create ++ subscription ++ poll)
      else
        Stream.raiseError(new IllegalStateException(s"Stream with topic ${topic.value} is already running or paused"))
    } yield records
  }

  override def pause: F[Unit] = pauseSignal.set(true)

  override def resume: F[Unit] = pauseSignal.set(false)

  //TODO: add logic with adminClient
  override def reset: F[Unit] = kafkaConsumer.read.map { consumer =>
    //val partitions = kafkaConsumer.assignment.asScala.filter(_.topic == topic.value)
    consumer.seekToBeginning(List.empty.asJava) //(partitions.asJava)
  }

  override def stop: F[Unit] = haltSignal.set(true)

  /**
    * Does only start of being assigned partitions after the first poll
    */
  private def subscribe(topic: Topic): F[Unit] = kafkaConsumer.read.map { consumer =>
    consumer.subscribe(List(topic.value).asJavaCollection)
  }
}
