package me.milan.clients.kafka

import scala.concurrent.duration._

import cats.effect.IO
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.clients.kafka.SchemaRegistryClient.Schema
import me.milan.config.{ ApplicationConfig, TestConfig }
import me.milan.domain.{ Key, Record, Topic }
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.Pub
import me.milan.pubsub.kafka.KProducer

class SchemaRegistrySpec extends WordSpec with Matchers with KafkaTestKit {
  import SchemaRegistrySpec._

  override val applicationConfig: ApplicationConfig = TestConfig.create(topic)

  "SchemaRegistryClient" can {

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    "GetAllSchema" should {

      "successfully retrieve all schemas" in {

        val schemas = for {
          _ ← kafkaAdminClient.createTopics
          _ ← IO.sleep(2.seconds)
          _ ← Pub.kafka[IO, Value].publish(record)
          schemas ← schemaRegistryClient.getAllSchemas
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
          _ ← kafkaAdminClient.createTopics
          _ ← IO.sleep(2.seconds)
          _ ← Pub.kafka[IO, Value].publish(record)
          _ ← schemaRegistryClient.deleteAllSchemas
          schemas ← schemaRegistryClient.getAllSchemas
        } yield schemas

        val result = schemas
          .unsafeRunTimed(15.seconds)
          .getOrElse(List.empty)

        result shouldBe empty
      }
    }
  }
}

object SchemaRegistrySpec {

  val topic = Topic("test")

  case class Value(value: String)
  val schema = Schema("test-me.milan.clients.kafka.SchemaRegistrySpec.Value")

  val key = Key("key1")
  val value = Value("value1")

  val record: Record[Value] = Record(topic, key, value, 0L)

}
