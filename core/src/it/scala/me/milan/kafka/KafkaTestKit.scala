package me.milan.kafka

import scala.concurrent.duration._

import cats.effect.IO
import org.scalatest.{ BeforeAndAfterEach, Suite }

import me.milan.config.ApplicationConfig
import me.milan.pubsub.kafka.KafkaAdminClient
import me.milan.serdes.kafka.SchemaRegistryClient

trait KafkaTestKit extends BeforeAndAfterEach {
  this: Suite ⇒

  val applicationConfig: ApplicationConfig

  implicit val executor = scala.concurrent.ExecutionContext.global
  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)

  val kafkaAdminClient = new KafkaAdminClient[IO](applicationConfig.kafka)
  val schemaRegistryClient = new SchemaRegistryClient[IO](applicationConfig.kafka)

  override def beforeEach(): Unit = {
    val program = for {
      _ ← kafkaAdminClient.deleteAllTopics
      _ ← schemaRegistryClient.deleteAllSchemas
      _ ← IO.sleep(500.millis)
    } yield ()

    program.unsafeRunTimed(10.seconds)
    ()
  }
}
