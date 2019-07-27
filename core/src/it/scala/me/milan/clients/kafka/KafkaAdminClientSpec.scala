package me.milan.clients.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.scalatest.{ Matchers, WordSpec }

import me.milan.kafka.{ Fixtures, KafkaTestKit }
import me.milan.pubsub.Sub
import me.milan.serdes.auto._

class KafkaAdminClientSpec extends WordSpec with Matchers with KafkaTestKit {
  import Fixtures._

  "KafkaAdminClient" can {

      "CreateTopics" should {

        "create topics successfully" in {

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(1.second)
            createdTopics <- kafkaAdminClient.getTopics()
          } yield createdTopics

          val result = program.unsafeRunTimed(10.seconds).get

          result shouldBe Set(fixtures.topic, fixtures.from, fixtures.to)
          result should have size 3

        }

        "create topics successfully although they already exist" in {

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(1.second)
            createdTopics <- kafkaAdminClient.getTopics()
          } yield createdTopics

          val result = program.unsafeRunTimed(10.seconds).get

          result shouldBe Set(fixtures.topic, fixtures.from, fixtures.to)
          result should have size 3

        }

      }

      "GetTopics" should {

        "not include system topics" in {

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(1.second)
            createdTopics <- kafkaAdminClient.getTopics()
          } yield createdTopics

          val result = program.unsafeRunTimed(10.seconds).get

          result shouldBe Set(fixtures.topic, fixtures.from, fixtures.to)
          result should have size 3

        }

        "include system topics" in {

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(1.second)
            createdTopics <- kafkaAdminClient.getTopics(ignoreSystemTopics = false)
          } yield createdTopics

          val result = program.unsafeRunTimed(10.seconds).get

          result should contain allElementsOf Set(fixtures.topic, fixtures.systemTopic)

        }

      }

      "DeleteTopics" should {

        "delete topics successfully" in {

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- kafkaAdminClient.deleteAllTopics
            _ <- IO.sleep(1.second)
            createdTopics <- kafkaAdminClient.getTopics()
          } yield createdTopics

          val result = program.unsafeRunTimed(10.seconds).get

          result shouldBe empty

        }

      }

      "ConsumerGroupMembers" should {

        "get all members successfully" in {

          val sub1 =
            Sub.kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic).unsafeRunSync
          val sub2 =
            Sub.kafka[IO, Value](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.topic).unsafeRunSync

          val startup = for {
            _ <- kafkaAdminClient.createTopics
          } yield ()

          val stream1 = sub1.start.compile.drain
          val stream2 = sub2.start.compile.drain

          val send = for {
            _ <- IO.sleep(1.seconds)
            consumerGroupMembers <- kafkaAdminClient.consumerGroupMembers(fixtures.consumerGroupId)
            _ <- sub1.stop
            _ <- sub2.stop
          } yield consumerGroupMembers

          val program = (stream1, stream2, startup, send)
            .parMapN { (_, _, _, result) =>
              result
            }

          val result = program.unsafeRunTimed(10.seconds).get

          result should have size 2

        }

      }

    }

}
