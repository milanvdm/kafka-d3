package me.milan.pubsub.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.either._
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.config.{ ApplicationConfig, KafkaConfig }
import me.milan.domain.Topic

class KafkaAdminClientIntegrationSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  import KafkaAdminClientIntegrationSpec._

  implicit val executor = scala.concurrent.ExecutionContext.global
  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)

  override def beforeEach(): Unit = {
    val program = for {
      appConfig ← IO.fromEither(applicationConfig.asRight)
      kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
      _ ← kafkaAdminClient.deleteAllTopics
      _ ← IO.sleep(500.millis)
    } yield ()

    program.unsafeRunTimed(10.seconds)
    ()
  }

  "KafkaAdminClient" can {

    "CreateTopics" should {

      "create topics successfully" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          createdTopic ← kafkaAdminClient.getTopics
        } yield createdTopic

        val result = program.unsafeRunTimed(10.seconds)

        topics should contain theSameElementsAs result.get

      }

      "create topics successfully although they already exist" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.createTopics
          createdTopic ← kafkaAdminClient.getTopics
        } yield createdTopic

        val result = program.unsafeRunTimed(10.seconds)

        topics should contain theSameElementsAs result.get

      }

    }

    "DeleteTopics" should {

      "delete topics successfully" in {

        val program = for {
          appConfig ← IO.fromEither(applicationConfig.asRight)
          kafkaAdminClient = new KafkaAdminClient[IO](appConfig.kafka)
          _ ← kafkaAdminClient.createTopics
          _ ← kafkaAdminClient.deleteAllTopics
          createdTopic ← kafkaAdminClient.getTopics
        } yield createdTopic

        val result = program.unsafeRunTimed(10.seconds).get

        result shouldBe empty

      }

    }

  }

}

object KafkaAdminClientIntegrationSpec {

  val topics: Set[Topic] = Set(Topic("test-topic"))

  val applicationConfig = ApplicationConfig(
    kafka = KafkaConfig(
      KafkaConfig.BootstrapServer("localhost:9092"),
      KafkaConfig.SchemaRegistryUrl(
        url = "http://localhost:8081"
      ),
      List(
        TopicConfig(
          name = Topic("test-topic"),
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        )
      )
    )
  )
}
