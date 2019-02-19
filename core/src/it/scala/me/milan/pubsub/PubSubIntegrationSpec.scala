package me.milan.pubsub

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.either._
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.config.{ ApplicationConfig, KafkaConfig }
import me.milan.domain.{ Key, Record, Topic }
import me.milan.pubsub.kafka.{ KProducer, KafkaAdminClient }

class PubSubIntegrationSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  import PubSubIntegrationSpec._

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

  "PubSub" can {

    val kafkaAdminClient = new KafkaAdminClient[IO](applicationConfig.kafka)

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    val sub = Sub.kafka[IO, Value](applicationConfig.kafka)

    "send one record type" should {

      "successfully receive the same record" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          result ← sub
            .poll(topic)
            .take(1)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(5.seconds)
          _ ← Pub.kafka[IO, Value1].publish(record1)
          _ ← sub.stop
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(15.seconds)
          .getOrElse(List.empty)
          .headOption

        result shouldBe Option(record1)

      }
    }

    "send two records with a different keys" should {

      "successfully receive the same record" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          result ← sub
            .poll(topic)
            .take(4)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(5.seconds)
          _ ← Pub.kafka[IO, Value1].publish(record1)
          _ ← Pub.kafka[IO, Value1].publish(record1)
          _ ← Pub.kafka[IO, Value2].publish(record2)
          _ ← Pub.kafka[IO, Value2].publish(record2)
          _ ← sub.stop
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(15.seconds)
          .getOrElse(List.empty)

        result should contain theSameElementsAs List(
          Record(topic, key1, value1, 0L),
          Record(topic, key1, value1, 0L),
          Record(topic, key2, value2, 0L),
          Record(topic, key2, value2, 0L)
        )

      }
    }

    "send two different record types" should {

      "successfully differentiate between 2 schemas" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          result ← sub
            .poll(topic)
            .take(2)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(5.seconds)
          _ ← Pub.kafka[IO, Value1].publish(record1)
          _ ← Pub.kafka[IO, Value2].publish(record2)
          _ ← sub.stop
        } yield ()

        (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(15.seconds)
          .getOrElse(List.empty)
          .map(record ⇒ (record.key, record.value))
          .foreach {
            case (_, value: Value1) ⇒
              value shouldBe value1
            case (_, value: Value2) ⇒
              value shouldBe value2
            case _ ⇒
              fail("pattern match failed on subtypes")
          }

      }
    }
  }

}

object PubSubIntegrationSpec {

  val topic = Topic("test")

  val applicationConfig = ApplicationConfig(
    kafka = KafkaConfig(
      KafkaConfig.BootstrapServer("localhost:9092"),
      KafkaConfig.SchemaRegistryUrl(
        url = "http://localhost:8081"
      ),
      List(
        TopicConfig(
          name = topic,
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        )
      )
    )
  )

  sealed trait Value
  case class Value1(value: String) extends Value
  case class Value2(value2: String) extends Value

  val key1 = Key("key1")
  val key2 = Key("key2")
  val value1 = Value1("value1")
  val value2 = Value2("value2")

  val record1: Record[Value1] = Record(topic, key1, value1, 0L)
  val record2: Record[Value2] = Record(topic, key2, value2, 0L)

}
