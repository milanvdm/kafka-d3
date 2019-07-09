package me.milan.pubsub

import java.time.OffsetDateTime

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }
import cats.syntax.monoid._
import cats.instances.list._
import cats.effect.IO._

import me.milan.config.{ ApplicationConfig, TestConfig }
import me.milan.domain.{ Key, Record, Topic }
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId
import me.milan.pubsub.kafka.KProducer

class SubSpec extends WordSpec with Matchers with KafkaTestKit {
  import SubSpec._

  override val applicationConfig: ApplicationConfig = TestConfig.create(topic)

  "Sub" can {

      implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
        .apply[IO](applicationConfig.kafka)
        .unsafeRunSync
        .producer

      "be stopped" should {

        "successfully start again" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic)
            .flatMap { sub =>
              val startup = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(1)
                  .compile
                  .toList
              } yield result

              val send1 = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.stop
              } yield ()

              val send2 = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value2].publish(record2)
                _ <- IO.sleep(1.seconds)
                _ <- sub.stop
              } yield ()

              val firstStart = (startup, send1)
                .parMapN { (result, _) =>
                  result
                }

              val secondStart = (startup, send2)
                .parMapN { (result, _) =>
                  result
                }

              firstStart.combine(secondStart)
            }

          val result = program
            .unsafeRunTimed(15.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            record1,
            record2
          )
        }

      }

      "be paused" should {

        "successfully receive the same records" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic)
            .flatMap { sub =>
              val startup = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(3)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.pause
                _ <- Pub.kafka[IO, Value2].publish(record2)
                _ <- Pub.kafka[IO, Value3].publish(record3)
                _ <- IO.sleep(1.seconds)
                _ <- sub.stop
              } yield ()

              (startup, send)
                .parMapN { (result, _) =>
                  result
                }
            }

          val result = program
            .unsafeRunTimed(15.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            record1
          )
        }

        "correctly stay part of the consumer-group" in {
          pending

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic)
            .flatMap { sub =>
              val startup = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(3)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.pause
                _ <- IO.sleep(5.seconds)
                consumerGroupMembers <- kafkaAdminClient.consumerGroupMembers(consumerGroupId)
                _ <- sub.stop
              } yield consumerGroupMembers

              (startup, send)
                .parMapN { (_, consumerGroupMembers) =>
                  consumerGroupMembers
                }
            }

          val result = program.unsafeRunTimed(15.seconds).get

          result should have size 2

        }
      }

      "be reset" should {

        "reprocess previous records" in {
          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic)
            .flatMap { sub =>
              val getMessages = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(3)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(record1)
                _ <- Pub.kafka[IO, Value2].publish(record2)
                _ <- Pub.kafka[IO, Value3].publish(record3)
                _ <- IO.sleep(1.second)
                _ <- sub.stop
              } yield ()

              val reset = for {
                _ <- sub.stop
                _ <- IO.sleep(5.seconds)
                _ <- kafkaAdminClient.resetConsumerGroup(consumerGroupId, topic, resetTime)
                _ <- IO.sleep(1.second)
              } yield ()

              val resultBeforeReset = (getMessages, send)
                .parMapN { (result, _) =>
                  result
                }

              val resultAfterReset = (getMessages, send)
                .parMapN { (result, _) =>
                  result
                }

              for {
                result1 <- resultBeforeReset
                _ <- reset
                result2 <- resultAfterReset
              } yield result1.combine(result2)

            }

          val result = program
            .unsafeRunTimed(20.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            record1,
            record2,
            record3,
            record1,
            record2,
            record3
          )

        }

        "reprocess only message after reset time" in {}

        "cannot be reset when running" in {}

        "reset a non existing topic" in {}

      }
    }
}

object SubSpec {

  val consumerGroupId = ConsumerGroupId("test")
  val topic = Topic("test")
  val resetTime: OffsetDateTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z")

  sealed trait Value
  case class Value1(value1: String) extends Value
  case class Value2(value2: String) extends Value
  case class Value3(value3: String) extends Value

  val key1 = Key("key1")
  val key2 = Key("key2")
  val key3 = Key("key3")
  val value1 = Value1("value1")
  val value2 = Value2("value2")
  val value3 = Value3("value3")

  val record1: Record[Value1] = Record(topic, key1, value1, 100L)
  val record2: Record[Value2] = Record(topic, key2, value2, 200L)
  val record3: Record[Value3] = Record(topic, key3, value3, 300L)

}
