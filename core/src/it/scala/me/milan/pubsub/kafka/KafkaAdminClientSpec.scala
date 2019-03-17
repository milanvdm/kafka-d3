package me.milan.pubsub.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.either._
import org.scalatest.{Matchers, WordSpec}

import me.milan.clients.kafka.KafkaAdminClient
import me.milan.config.{ApplicationConfig, Config}
import me.milan.domain.Topic
import me.milan.kafka.KafkaTestKit

class KafkaAdminClientSpec extends WordSpec with Matchers with KafkaTestKit {
  import KafkaAdminClientSpec._

  override val applicationConfig: ApplicationConfig = Config.create(topic, systemTopic)

  "KafkaAdminClient" can {

    "CreateTopics" should {

      "create topics successfully" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result should have size 1

      }

      "create topics successfully although they already exist" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
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
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result.contains(topic) shouldBe true
        result should have size 1

      }

      "include system topics" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
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
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.deleteAllTopics
          createdTopics ← kafkaAdminClient.getTopics()
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result shouldBe empty

      }

    }

  }

}

object KafkaAdminClientSpec {

  val topic = Topic("test")
  val systemTopic = Topic("_system")

}
