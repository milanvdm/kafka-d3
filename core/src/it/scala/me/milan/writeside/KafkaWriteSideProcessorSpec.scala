package me.milan.writeside

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{Matchers, WordSpec}

import me.milan.config.{ApplicationConfig, Config}
import me.milan.domain._
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.kafka.{KProducer, KafkaAdminClient}
import me.milan.pubsub.{Pub, Sub}

class KafkaWriteSideProcessorSpec extends WordSpec with Matchers with KafkaTestKit {
  import KafkaWriteSideProcessorSpec._
  import events.UserEvents._

  override val applicationConfig: ApplicationConfig = Config.create(from, to)

  "KafkaWriteSideProcessor" can {

    val kafkaAdminClient = new KafkaAdminClient[IO](applicationConfig.kafka)

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    val sub = Sub.kafka[IO, UserState](applicationConfig.kafka)

    val writeSideProcessor = WriteSideProcessor
      .kafka[IO, UserState, UserEvent](
        applicationConfig.kafka,
        UserAggregator,
        "test",
        from,
        to
      )

    "handle create and update events" should {

      "successfully receive the correct end state" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          _ ← writeSideProcessor.start
          result ← sub
            .poll(to)
            .take(2)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(1.seconds)
          _ ← Pub.kafka[IO, UserCreated].publish(created)
          _ ← Pub.kafka[IO, UserUpdated].publish(updated)
          _ ← IO.sleep(5.seconds)
          _ ← sub.stop
          _ ← writeSideProcessor.stop
          _ ← IO.sleep(5.seconds)
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(20.seconds)
          .getOrElse(List.empty)
          .lastOption
          .map(_.value)

        result shouldBe Some(User(userId, "Milan1", "updated"))

      }
    }

    "handle create, update, and delete events" should {

      "successfully receive the correct end state" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          _ ← writeSideProcessor.start
          result ← sub
            .poll(to)
            .take(3)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(1.seconds)
          _ ← Pub.kafka[IO, UserCreated].publish(created)
          _ ← Pub.kafka[IO, UserUpdated].publish(updated)
          _ ← Pub.kafka[IO, UserRemoved].publish(removed)
          _ ← IO.sleep(5.seconds)
          _ ← sub.stop
          _ ← writeSideProcessor.stop
          _ ← IO.sleep(5.seconds)
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(20.seconds)
          .getOrElse(List.empty)
          .lastOption
          .map(_.value)

        result shouldBe Some(null)

      }
    }

    "handle multiple users and events" should {

      "successfully receive the correct end state" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          _ ← writeSideProcessor.start
          result ← sub
            .poll(to)
            .take(4)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(1.seconds)
          _ ← Pub.kafka[IO, UserCreated].publish(created)
          _ ← Pub.kafka[IO, UserUpdated].publish(updated)
          _ ← Pub.kafka[IO, UserCreated].publish(created2)
          _ ← Pub.kafka[IO, UserUpdated].publish(updated2)
          _ ← IO.sleep(5.seconds)
          _ ← sub.stop
          _ ← writeSideProcessor.stop
          _ ← IO.sleep(5.seconds)
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(20.seconds)
          .getOrElse(List.empty)
          .map(_.value)

        result should contain theSameElementsAs List(
          User(userId, "Milan", "created"),
          User(userId, "Milan1", "updated"),
          User(userId2, "Milan2", "created"),
          User(userId2, "Milan3", "updated")
        )

      }
    }

    "handle a stop and start" should {

      "successfully receive the correct end state" in {

        val startup = for {
          _ ← kafkaAdminClient.createTopics
          _ ← writeSideProcessor.start
          result ← sub
            .poll(to)
            .take(2)
            .compile
            .toList
        } yield result

        val send = for {
          _ ← IO.sleep(1.seconds)
          _ ← Pub.kafka[IO, UserCreated].publish(created)
          _ ← IO.sleep(3.seconds)
          _ ← writeSideProcessor.stop
          _ ← IO.sleep(3.seconds)
          _ ← Pub.kafka[IO, UserUpdated].publish(updated)
          _ ← writeSideProcessor.start
          _ ← IO.sleep(10.seconds)
          _ ← sub.stop
          _ ← writeSideProcessor.stop
          _ ← IO.sleep(5.seconds)
        } yield ()

        val result = (startup, send)
          .parMapN { (result, _) ⇒
            result
          }
          .unsafeRunTimed(25.seconds)
          .getOrElse(List.empty)
          .lastOption
          .map(_.value)

        result shouldBe Some(User(userId, "Milan1", "updated"))

      }
    }
  }
}

object KafkaWriteSideProcessorSpec {
  import events.UserEvents._

  val from = Topic("from")
  val to = Topic("to")

  val created: Record[UserCreated] = Record(from, userId, UserCreated(userId, "Milan"), 0)
  val updated: Record[UserUpdated] = Record(from, userId, UserUpdated(userId, "Milan1"), 1)
  val removed: Record[UserRemoved] = Record(from, userId, UserRemoved(userId), 2)

  val created2: Record[UserCreated] = Record(from, userId2, UserCreated(userId2, "Milan2"), 0)
  val updated2: Record[UserUpdated] = Record(from, userId2, UserUpdated(userId2, "Milan3"), 1)

}
