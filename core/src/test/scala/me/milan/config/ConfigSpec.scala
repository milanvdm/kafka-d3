package me.milan.config

import org.scalatest.{ Matchers, WordSpec }

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.Topic
import me.milan.pubsub.kafka.KafkaBoot

class ConfigSpec extends WordSpec with Matchers {
  import ConfigSpec._

  "Configuration" can {

    "ApplicationConfig" should {

      "be correctly parsed" in {

        val result = KafkaBoot.parseConfig

        result shouldBe Right(applicationConfig)

      }
    }
  }
}

object ConfigSpec {

  val applicationConfig = ApplicationConfig(
    kafka = KafkaConfig(
      KafkaConfig.BootstrapServer("test"),
      KafkaConfig.SchemaRegistryUrl(
        url = "test"
      ),
      List(
        TopicConfig(
          name = Topic("test"),
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        )
      )
    ),
    writeSide = WriteSideConfig(
      WriteSideConfig.UrlPath("system")
    )
  )
}
