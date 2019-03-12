package me.milan.pubsub.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.either._
import org.scalatest.{Matchers, WordSpec}

import me.milan.config.{ApplicationConfig, Config}
import me.milan.domain.Topic
import me.milan.kafka.KafkaTestKit

class KafkaAdminClientIntegrationSpec extends WordSpec with Matchers with KafkaTestKit {
  import KafkaAdminClientIntegrationSpec._

  override val applicationConfig: ApplicationConfig = Config.create(topic)

  "KafkaAdminClient" can {

    "CreateTopics" should {

      "create topics successfully" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds)

        result.get.contains(topic) should be true

      }

      "create topics successfully although they already exist" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.createTopics
          createdTopics ← kafkaAdminClient.getTopics
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds)

        result.get.contains(topic) should be true

      }

    }

    "DeleteTopics" should {

      "delete topics successfully" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.deleteAllTopics
          createdTopics ← kafkaAdminClient.getTopics
        } yield createdTopics

        val result = program.unsafeRunTimed(10.seconds).get

        result shouldBe empty

      }

    }

  }

}

object KafkaAdminClientIntegrationSpec {

  val topic = Topic("test")

}
