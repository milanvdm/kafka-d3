package me.milan.config

import scala.concurrent.duration._

import cats.effect.IO
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.http4s.Uri
import org.scalatest.{ Matchers, WordSpec }

import me.milan.config.KafkaConfig.{ ConsumerConfig, TopicConfig }
import me.milan.domain.Topic

class TestConfigSpec extends WordSpec with Matchers {
  import TestConfigSpec._

  "Configuration" can {

      "ApplicationConfig" should {

        "be correctly parsed" in {

          val result = Config.stream[IO].compile.lastOrError.unsafeRunSync

          result shouldBe applicationConfig

        }
      }
    }
}

object TestConfigSpec {

  val applicationConfig = ApplicationConfig(
    kafka = KafkaConfig(
      Set(
        KafkaConfig.BootstrapServer(Uri.unsafeFromString("test1:8080")),
        KafkaConfig.BootstrapServer(Uri.unsafeFromString("test2:8080")),
        KafkaConfig.BootstrapServer(Uri.unsafeFromString("test3:8080"))
      ),
      KafkaConfig.SchemaRegistryConfig(
        uri = Uri.unsafeFromString("http://test.com"),
        identityMapCapacity = 2000
      ),
      List(
        TopicConfig(
          name = Topic("test"),
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1),
          retention = 1.second
        )
      ),
      ConsumerConfig(
        maxPollInterval = 1.second
      )
    ),
    writeSide = WriteSideConfig(
      WriteSideConfig.UrlPath("system"),
      AutoOffsetReset.LATEST
    )
  )
}
