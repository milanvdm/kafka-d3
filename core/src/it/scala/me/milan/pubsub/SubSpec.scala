package me.milan.pubsub

import java.time.OffsetDateTime

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.IO._
import cats.instances.list._
import cats.syntax.monoid._
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.domain.{ Error, Topic }
import me.milan.kafka.{ Fixtures, KafkaTestKit }
import me.milan.pubsub.kafka.KProducer
import me.milan.serdes.auto._

class SubSpec extends WordSpec with Matchers with KafkaTestKit {
  import Fixtures._
  import SubSpec._

  "Sub" can {

      implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
        .apply[IO](applicationConfig.kafka)
        .unsafeRunSync
        .producer

      "be stopped" should {

        "successfully start again" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
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
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.stop
              } yield ()

              val send2 = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
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
            fixtures.record1,
            fixtures.record2
          )
        }

      }

      "be paused" should {

        "successfully receive the same records" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
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
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.pause
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
                _ <- Pub.kafka[IO, Value3].publish(fixtures.record3)
                _ <- IO.sleep(1.seconds)
                _ <- sub.stop
              } yield ()

              (startup, send)
                .parMapN { (result, _) =>
                  result
                }
            }

          val result = program
            .unsafeRunTimed(20.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            fixtures.record1
          )
        }

        "correctly stay part of the consumer-group" in {
          pending

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
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
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- IO.sleep(1.seconds)
                _ <- sub.pause
                _ <- IO.sleep(5.seconds)
                consumerGroupMembers <- kafkaAdminClient.consumerGroupMembers(fixtures.consumerGroupId)
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
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
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
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
                _ <- Pub.kafka[IO, Value3].publish(fixtures.record3)
                _ <- IO.sleep(1.second)
              } yield ()

              val reset = for {
                _ <- sub.stop
                _ <- IO.sleep(1.seconds)
                _ <- kafkaAdminClient.resetConsumerGroup(fixtures.consumerGroupId, fixtures.topic, epoch)
                _ <- IO.sleep(1.second)
              } yield ()

              val resultBeforeReset = (getMessages, send)
                .parMapN { (result, _) =>
                  result
                }

              val resultAfterReset = getMessages

              for {
                result1 <- resultBeforeReset
                _ <- reset
                result2 <- resultAfterReset
                _ <- sub.stop
              } yield result1.combine(result2)

            }

          val result = program
            .unsafeRunTimed(20.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            fixtures.record1,
            fixtures.record2,
            fixtures.record3,
            fixtures.record1,
            fixtures.record2,
            fixtures.record3
          )

        }

        "reprocess only messages after reset time" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
            .flatMap { sub =>
              val getMessages = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(3)
                  .interruptAfter(10.seconds)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
                _ <- Pub.kafka[IO, Value3].publish(fixtures.record3)
                _ <- IO.sleep(1.second)
              } yield ()

              val reset = for {
                _ <- sub.stop
                _ <- IO.sleep(1.seconds)
                _ <- kafkaAdminClient.resetConsumerGroup(fixtures.consumerGroupId, fixtures.topic, resetTime)
                _ <- IO.sleep(1.second)
              } yield ()

              val resultBeforeReset = (getMessages, send)
                .parMapN { (result, _) =>
                  result
                }

              val resultAfterReset = getMessages

              for {
                result1 <- resultBeforeReset
                _ <- reset
                result2 <- resultAfterReset
                _ <- sub.stop
              } yield result1.combine(result2)

            }

          val result = program
            .unsafeRunTimed(20.seconds)
            .getOrElse(List.empty)

          result should contain theSameElementsAs List(
            fixtures.record1,
            fixtures.record2,
            fixtures.record3,
            fixtures.record2,
            fixtures.record3
          )

        }
      }

      "cannot be reset when running" in {

        val program = Sub
          .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
          .flatMap { sub =>
            val getMessages = for {
              _ <- kafkaAdminClient.createTopics
              result <- sub.start
                .take(1)
                .compile
                .toList
            } yield result

            val send = for {
              _ <- IO.sleep(5.seconds)
              _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
              _ <- IO.sleep(1.second)
            } yield ()

            val reset = for {
              _ <- IO.sleep(1.seconds)
              _ <- kafkaAdminClient.resetConsumerGroup(fixtures.consumerGroupId, fixtures.topic, resetTime)
              _ <- IO.sleep(1.second)
            } yield ()

            (getMessages, send, reset)
              .parMapN { (result, _, _) =>
                result
              }
              .flatMap(_ => sub.stop)

          }

        val thrown = the[java.lang.IllegalStateException] thrownBy {
              program.unsafeRunTimed(10.seconds)
            }

        thrown.getMessage shouldBe "Cannot reset a running consumer-group"

      }

      "cannot be reset on a non existing topic" in {

        val program = kafkaAdminClient.resetConsumerGroup(fixtures.consumerGroupId, Topic("non-existing"), resetTime)

        the[Error.TopicNotFound.type] thrownBy {
          program.unsafeRunTimed(10.seconds)
        }

      }
    }
}

object SubSpec {

  val epoch: OffsetDateTime = OffsetDateTime.parse("1970-01-01T00:00:00.000Z")
  val resetTime: OffsetDateTime = OffsetDateTime.parse("1970-01-01T00:00:00.150Z")

}
