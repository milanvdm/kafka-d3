package me.milan.writeside

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ Matchers, WordSpec }

import me.milan.domain._
import me.milan.kafka.KafkaTestKit
import me.milan.pubsub.kafka.KProducer
import me.milan.pubsub.{ Pub, Sub }
import me.milan.serdes.auto._

class KafkaTtlWriteSideProcessorSpec extends WordSpec with Matchers with KafkaTestKit {
  import events.UserEvents._

  "KafkaTtlWriteSideProcessor" can {

      implicit lazy val kafkaProducer: KafkaProducer[String, GenericRecord] = KProducer
        .apply[IO](applicationConfig.kafka)
        .unsafeRunSync
        .producer

      "handle out-of-order events" should {

        "successfully receive the correct end state" in {

          val sub =
            Sub.kafka[IO, UserState](applicationConfig.kafka, fixtures.consumerGroupId, fixtures.to).unsafeRunSync()

          val writeSideProcessor = WriteSideProcessor
            .kafkaTimeToLive[IO, UserState, UserEvent](
              applicationConfig.kafka,
              applicationConfig.writeSide,
              UserAggregator,
              "KafkaTtlWriteSideProcessorSpec",
              fixtures.from,
              fixtures.to,
              1.millis
            )

          val created: Record[UserCreated] = Record(fixtures.from, userId, UserCreated(userId, "Milan"), 0)
          val updated: Record[UserUpdated] = Record(fixtures.from, userId, UserUpdated(userId, "Milan1"), 1)

          val updatedDelayed3: Record[UserUpdated] =
            Record(fixtures.from, userId, UserUpdated(userId, "Milan3"), 3)
          val updatedDelayed2: Record[UserUpdated] =
            Record(fixtures.from, userId, UserUpdated(userId, "Milan2"), 2)

          val startup = for {
            _ <- kafkaAdminClient.createTopics
            _ <- writeSideProcessor.start
            result <- sub.start
              .take(3)
              .compile
              .toList
          } yield result

          val send = for {
            _ <- IO.sleep(1.seconds)
            _ <- Pub.kafka[IO, UserCreated].publish(created) // 0
            _ <- Pub.kafka[IO, UserUpdated].publish(updatedDelayed3) // 3
            _ <- Pub.kafka[IO, UserUpdated].publish(updatedDelayed2) // 2 -> 1 ms late, still accepted
            _ <- Pub.kafka[IO, UserUpdated].publish(updated) // 1 -> 2 ms late, so rejected
            _ <- IO.sleep(5.seconds)
            _ <- sub.stop
            _ <- writeSideProcessor.stop
            _ <- IO.sleep(5.seconds)
          } yield ()

          val result = (startup, send)
            .parMapN { (result, _) =>
              result
            }
            .unsafeRunTimed(20.seconds)
            .getOrElse(List.empty)
            .lastOption
            .map(_.value)

          result shouldBe Some(User(userId, "Milan2", "updated"))

        }
      }
    }
}
