package me.milan.pubsub

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.config.{ ApplicationConfig, Config }
import me.milan.domain.{ Key, Record, Topic }
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.kafka.KProducer

class PubSubSpec extends WordSpec with Matchers with KafkaTestKit {
  import PubSubSpec._

  override val applicationConfig: ApplicationConfig = Config.create(topic)

  "PubSub" can {

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    "send one record type" should {

      "successfully receive the same record" in {

        val sub = Sub.kafka[IO, Value](applicationConfig.kafka)

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

    "send two records with different keys" should {

      "successfully receive the same record" in {

        val sub = Sub.kafka[IO, Value](applicationConfig.kafka)

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

        val sub = Sub.kafka[IO, Value](applicationConfig.kafka)

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

object PubSubSpec {

  val topic = Topic("test")

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
