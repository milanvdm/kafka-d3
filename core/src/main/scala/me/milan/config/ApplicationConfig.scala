package me.milan.config

import me.milan.config.WriteSideConfig.UrlPath
import me.milan.domain.Topic

case class ApplicationConfig(
  kafka: KafkaConfig,
  writeSide: WriteSideConfig
)

case class KafkaConfig(
  bootstrapServer: KafkaConfig.BootstrapServer,
  schemaRegistry: KafkaConfig.SchemaRegistryUrl,
  topics: List[KafkaConfig.TopicConfig]
)

object KafkaConfig {

  case class BootstrapServer(value: String) extends AnyVal
  case class SchemaRegistryUrl(url: String) extends AnyVal

  case class TopicConfig(
    name: Topic,
    partitions: TopicConfig.Partitions,
    replicationFactor: TopicConfig.ReplicationFactor
  )

  object TopicConfig {
    case class Partitions(value: Int) extends AnyVal
    case class ReplicationFactor(value: Int) extends AnyVal
  }

}

case class WriteSideConfig(urlPath: UrlPath)

object WriteSideConfig {

  case class UrlPath(value: String) extends AnyVal

}
