package me.milan.pubsub

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.kafka.{ Fixtures, KafkaTestKit }
import me.milan.pubsub.kafka.KProducer

class PubSubSpec extends WordSpec with Matchers with KafkaTestKit {
  import Fixtures._

  "PubSub" can {

      implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
        .apply[IO](applicationConfig.kafka)
        .unsafeRunSync
        .producer

      "send one record type" should {

        "successfully receive the same record" in {

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

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
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
            .headOption

          result shouldBe Option(fixtures.record1)
        }
      }

      "send two records with different keys" should {

        "successfully receive the same record" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
            .flatMap { sub =>
              val startup = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(4)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
                _ <- IO.sleep(2.seconds)
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
            fixtures.record1,
            fixtures.record1,
            fixtures.record2,
            fixtures.record2
          )

        }
      }

      "send two different record types" should {

        "successfully differentiate between 2 schemas" in {

          val program = Sub
            .kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic)
            .flatMap { sub =>
              val startup = for {
                _ <- kafkaAdminClient.createTopics
                result <- sub.start
                  .take(2)
                  .compile
                  .toList
              } yield result

              val send = for {
                _ <- IO.sleep(5.seconds)
                _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
                _ <- Pub.kafka[IO, Value2].publish(fixtures.record2)
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

          result
            .map(record => (record.key, record.value))
            .foreach {
              case (_, value: Value1) =>
                value shouldBe fixtures.value1
              case (_, value: Value2) =>
                value shouldBe fixtures.value2
              case _ =>
                fail("pattern match failed on subtypes")
            }

          result should have size 2
        }
      }
    }

}
