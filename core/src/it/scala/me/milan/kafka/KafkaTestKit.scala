package me.milan.kafka

import scala.concurrent.duration._

import cats.effect.IO
import org.scalatest.{ BeforeAndAfterEach, Suite }

import me.milan.clients.kafka.{ KafkaAdminClient, SchemaRegistryClient }
import me.milan.config.ApplicationConfig

trait KafkaTestKit extends BeforeAndAfterEach {
  this: Suite =>

  val applicationConfig: ApplicationConfig

  implicit val executor = scala.concurrent.ExecutionContext.global
  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)

  lazy val kafkaAdminClient: KafkaAdminClient[IO] = KafkaAdminClient[IO](applicationConfig.kafka).unsafeRunSync
  lazy val schemaRegistryClient: SchemaRegistryClient[IO] =
    SchemaRegistryClient[IO](applicationConfig.kafka).unsafeRunSync

  override def beforeEach(): Unit = {
    val program = for {
      _ <- schemaRegistryClient.deleteAllSchemas
      _ <- kafkaAdminClient.deleteAllTopics
      _ <- IO.sleep(1.seconds)
    } yield ()

    program.unsafeRunTimed(2.seconds)
    ()
  }
}
