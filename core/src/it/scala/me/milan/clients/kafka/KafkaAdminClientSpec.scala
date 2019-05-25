package me.milan.clients.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.scalatest.{ Matchers, WordSpec }

import me.milan.config.{ ApplicationConfig, TestConfig }
import me.milan.domain.Topic
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.Sub
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId

class KafkaAdminClientSpec extends WordSpec with Matchers with KafkaTestKit {
  import KafkaAdminClientSpec._

  override val applicationConfig: ApplicationConfig = TestConfig.create(topic, systemTopic)

  "KafkaAdminClient" can {

    "CreateTopics" should {

      "create topics successfully" in {

        val program = for {
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result should have size 1

      }

      "create topics successfully although they already exist" in {

        val program = for {
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result should have size 1

      }

    }

    "GetTopics" should {

      "not include system topics" in {

        val program = for {
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result should have size 1

      }

      "include system topics" in {

        val program = for {
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics(ignoreSystemTopics = false)
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result.size should be > 1

      }

    }

    "DeleteTopics" should {

      "delete topics successfully" in {

        val program = for {
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.deleteAllTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result shouldBe empty

      }

    }

    "ConsumerGroupMembers" should {

      "get all members successfully" in {

        val sub1 = Sub.kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic).unsafeRunSync
        val sub2 = Sub.kafka[IO, Value](applicationConfig.kafka, consumerGroupId, topic).unsafeRunSync

        val startup = for {
          _ ← kafkaAdminClient.createTopics
        } yield ()

        val stream1 = sub1.start.compile.drain
        val stream2 = sub2.start.compile.drain

        val send = for {
          _ ← IO.sleep(1.seconds)
          consumerGroupMembers ← kafkaAdminClient.consumerGroupMembers(consumerGroupId)
          _ ← sub1.stop
          _ ← sub2.stop
        } yield consumerGroupMembers

        val program = (stream1, stream2, startup, send)
          .parMapN { (_, _, _, result) ⇒
            result
          }

        val result = program.unsafeRunTimed(10.seconds).get

        result should have size 2

      }

    }

  }

}

object KafkaAdminClientSpec {

  val consumerGroupId = ConsumerGroupId("test")
  val topic = Topic("test")
  val systemTopic = Topic("_system")

  sealed trait Value
  case class Value1(value: String) extends Value
  case class Value2(value2: String) extends Value

}
