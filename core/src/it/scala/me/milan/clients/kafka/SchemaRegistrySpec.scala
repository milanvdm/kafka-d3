package me.milan.clients.kafka

import scala.concurrent.duration._

import cats.effect.IO
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.clients.kafka.SchemaRegistryClient.Schema
import me.milan.kafka.{ Fixtures, KafkaTestKit }
import me.milan.pubsub.Pub
import me.milan.pubsub.kafka.KProducer

class SchemaRegistrySpec extends WordSpec with Matchers with KafkaTestKit {
  import Fixtures._

  "SchemaRegistryClient" can {

      implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
        .apply[IO](applicationConfig.kafka)
        .unsafeRunSync
        .producer

      "GetAllSchema" should {

        "successfully retrieve all schemas" in {

          val schema = Schema(s"${fixtures.topic.value}-me.milan.kafka.Fixtures.Value1")

          val schemas = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(2.seconds)
            _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
            schemas <- schemaRegistryClient.getAllSchemas
          } yield schemas

          val result = schemas
            .unsafeRunTimed(15.seconds)
            .getOrElse(List.empty)

          result should have size 1
          result.head shouldBe schema
        }
      }

      "DeleteAllSchemas" should {

        "successfully delete all schemas" in {

          val schemas = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(2.seconds)
            _ <- Pub.kafka[IO, Value1].publish(fixtures.record1)
            _ <- schemaRegistryClient.deleteAllSchemas
            schemas <- schemaRegistryClient.getAllSchemas
          } yield schemas

          val result = schemas
            .unsafeRunTimed(15.seconds)
            .getOrElse(List.empty)

          result shouldBe empty
        }
      }
    }
}
