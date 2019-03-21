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

class SubSpec extends WordSpec with Matchers with KafkaTestKit {
  import SubSpec._

  override val applicationConfig: ApplicationConfig = Config.create(topic)

  "Sub" can {

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    "be paused" should {

      "successfully receive the same records" in {

        val program = Sub
          .kafka[IO, Value](applicationConfig.kafka, topic)
          .flatMap { sub ⇒
            val startup = for {
              _ ← kafkaAdminClient.createTopics
              result ← sub.start
                .take(3)
                .compile
                .toList
            } yield result

            val send = for {
              _ ← IO.sleep(5.seconds)
              _ ← Pub.kafka[IO, Value1].publish(record1)
              _ ← IO.sleep(1.seconds)
              _ ← sub.pause
              _ ← Pub.kafka[IO, Value1].publish(record1)
              _ ← Pub.kafka[IO, Value1].publish(record1)
              _ ← IO.sleep(1.seconds)
              _ ← sub.stop
            } yield ()

            (startup, send)
              .parMapN { (result, _) ⇒
                result
              }
          }

        val result = program
          .unsafeRunTimed(15.seconds)
          .getOrElse(List.empty)

        result should contain theSameElementsAs List(
          record1
        )
      }
    }
  }
}

object SubSpec {

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
