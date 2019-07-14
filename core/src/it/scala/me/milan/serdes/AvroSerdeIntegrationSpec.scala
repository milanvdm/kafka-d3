package me.milan.serdes

import scala.concurrent.duration._

import cats.effect.IO
import com.sksamuel.avro4s.AvroName
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.domain.{ Key, Record }
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.Pub
import me.milan.pubsub.kafka.KProducer

class AvroSerdeIntegrationSpec extends WordSpec with Matchers with KafkaTestKit {
  import AvroSerdeIntegrationSpec._

  "AvroSerde" can {

      "send a backwards compatible record type" should {

        "successfully register the backwards compatible schema" in {

          val record: Record[Value1] = Record(fixtures.topic, key1, value1, 0L)
          val recordWithBackwardsCompatibility: Record[NewValue1] = Record(fixtures.topic, key1, newValue, 0L)

          implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
            .apply[IO](applicationConfig.kafka)
            .unsafeRunSync
            .producer

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(2.seconds)
            _ <- Pub.kafka[IO, Value1].publish(record)
            _ <- Pub.kafka[IO, NewValue1].publish(recordWithBackwardsCompatibility)
          } yield ()

          program.unsafeRunTimed(10.seconds)

        }
      }

      "send a non backwards compatible record" should {

        "throw a SerializationException" in {

          val record: Record[Value1] = Record(fixtures.topic, key1, value1, 0L)
          val recordWithBreakingCompatibility: Record[BreakingValue1] =
            Record(fixtures.topic, fixtures.key1, breakingValue, 0L)

          implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
            .apply[IO](applicationConfig.kafka)
            .unsafeRunSync
            .producer

          val program = for {
            _ <- kafkaAdminClient.createTopics
            _ <- IO.sleep(2.seconds)
            _ <- Pub.kafka[IO, Value1].publish(record)
            _ <- Pub.kafka[IO, BreakingValue1].publish(recordWithBreakingCompatibility)
          } yield ()

          val thrown = the[org.apache.kafka.common.errors.SerializationException] thrownBy {
                program.unsafeRunTimed(10.seconds)
              }

          thrown.getMessage shouldBe
            "Error registering Avro schema: {\"type\":\"record\",\"name\":\"Value1\",\"namespace\":\"me.milan.serdes.AvroSerdeIntegrationSpec\",\"fields\":[{\"name\":\"newValue\",\"type\":\"int\"}]}"

        }
      }
    }

}

object AvroSerdeIntegrationSpec {

  trait Value
  case class Value1(value: String) extends Value
  @AvroName("Value1")
  case class NewValue1(
    value: String,
    newValue: Option[String] = None
  ) extends Value
  @AvroName("Value1")
  case class BreakingValue1(newValue: Int) extends Value

  val key1 = Key("key1")
  val value1 = Value1("value1")
  val newValue = NewValue1("value1", Some("test"))
  val breakingValue = BreakingValue1(1)

}
