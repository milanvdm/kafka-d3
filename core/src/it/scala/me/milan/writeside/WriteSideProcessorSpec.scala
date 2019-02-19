package me.milan.writeside

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.either._
import cats.syntax.parallel._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.config.{ ApplicationConfig, KafkaConfig }
import me.milan.domain._
import me.milan.pubsub.kafka.{ KProducer, KafkaAdminClient }
import me.milan.pubsub.{ Pub, Sub }

class WriteSideProcessorSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  import WriteSideProcessorSpec._

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

  "KafkaTtlWriteSideProcessor" can {

    val kafkaAdminClient = new KafkaAdminClient[IO](applicationConfig.kafka)

    implicit val kafkaProducer: KafkaProducer[String, GenericRecord] =
      new KProducer(applicationConfig.kafka).producer

    val sub = Sub.kafka[IO, UserState](applicationConfig.kafka)

    val writeSideProcessor = WriteSideProcessor
      .kafkaTimeToLive[IO, UserState, UserEvent](
        applicationConfig.kafka,
        UserAggregator,
        "test",
        from,
        to,
        1.millis
      )

    "handle out-of-order events" should {

      "successfully receive the correct end state" in {

        val updatedDelayed3: Record[UserUpdated] =
          Record(from, userId, UserUpdated(userId, "Milan3"), 3)
        val updatedDelayed2: Record[UserUpdated] =
          Record(from, userId, UserUpdated(userId, "Milan2"), 2)

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
          _ ← Pub.kafka[IO, UserCreated].publish(created) // 0
          _ ← Pub.kafka[IO, UserUpdated].publish(updatedDelayed3) // 3
          _ ← Pub.kafka[IO, UserUpdated].publish(updatedDelayed2) // 2 -> 1 ms late, still accepted
          _ ← Pub.kafka[IO, UserUpdated].publish(updated) // 1 -> 2 ms late, so rejected
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

        result shouldBe Some(User(userId, "Milan2", "updated"))

      }
    }
  }
}

object WriteSideProcessorSpec {

  val userId = Key.generate
  val userId2 = Key.generate
  val from = Topic("from")
  val to = Topic("to")

  val applicationConfig = ApplicationConfig(
    kafka = KafkaConfig(
      KafkaConfig.BootstrapServer("localhost:9092"),
      KafkaConfig.SchemaRegistryUrl(
        url = "http://localhost:8081"
      ),
      List(
        TopicConfig(
          name = from,
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        ),
        TopicConfig(
          name = to,
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        )
      )
    )
  )

  sealed trait UserEvent
  case class UserCreated(
    id: Key,
    name: String
  ) extends UserEvent
  case class UserUpdated(
    id: Key,
    name: String
  ) extends UserEvent
  case class UserRemoved(id: Key) extends UserEvent

  sealed trait UserState
  case class User(
    id: Key,
    name: String,
    status: String
  ) extends UserState
  case class UserTomb(id: Key) extends UserState with TombStone

  case object UserAggregator extends Aggregator[UserState, UserEvent] {

    override def process(
      previous: Option[UserState],
      event: UserEvent
    ): UserState = event match {
      case UserCreated(key, name) ⇒
        User(key, name, "created")
      case UserUpdated(key, name) ⇒
        User(key, name, "updated")
      case UserRemoved(id) ⇒
        UserTomb(id)
    }
  }

  val created: Record[UserCreated] = Record(from, userId, UserCreated(userId, "Milan"), 0)
  val updated: Record[UserUpdated] = Record(from, userId, UserUpdated(userId, "Milan1"), 1)
  val removed: Record[UserRemoved] = Record(from, userId, UserRemoved(userId), 2)

  val created2: Record[UserCreated] = Record(from, userId2, UserCreated(userId2, "Milan2"), 0)
  val updated2: Record[UserUpdated] = Record(from, userId2, UserUpdated(userId2, "Milan3"), 1)

}
