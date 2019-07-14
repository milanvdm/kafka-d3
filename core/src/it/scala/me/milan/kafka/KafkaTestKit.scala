package me.milan.kafka

import scala.concurrent.duration._

import cats.effect.IO
import org.scalatest.{ BeforeAndAfterEach, Suite }

import me.milan.clients.kafka.{ KafkaAdminClient, SchemaRegistryClient }
import me.milan.config.{ ApplicationConfig, TestConfig }
import me.milan.domain.Topic

trait KafkaTestKit extends BeforeAndAfterEach {
  this: Suite =>

  implicit val executor = scala.concurrent.ExecutionContext.global
  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)

  var fixtures: Fixtures = _

  var topics: List[Topic] = _

  var applicationConfig: ApplicationConfig = _
  var kafkaAdminClient: KafkaAdminClient[IO] = _
  var schemaRegistryClient: SchemaRegistryClient[IO] = _

  override def beforeEach(): Unit = {
    fixtures = new Fixtures

    topics = List(fixtures.topic, fixtures.from, fixtures.to, fixtures.systemTopic)

    applicationConfig = TestConfig.create(topics: _*)
    kafkaAdminClient = KafkaAdminClient[IO](applicationConfig.kafka).unsafeRunSync
    schemaRegistryClient = SchemaRegistryClient[IO](applicationConfig.kafka).unsafeRunSync

    cleanup()
  }

  private def cleanup(): Unit = {
    val program = for {
      _ <- schemaRegistryClient.deleteAllSchemas
      _ <- kafkaAdminClient.deleteAllTopics
      _ <- IO.sleep(1.seconds)
    } yield ()

    program.unsafeRunSync()
  }
}
