package me.milan.config

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.Topic

object Config {

  val bootstrapServers: Set[KafkaConfig.BootstrapServer] = Set(
    KafkaConfig.BootstrapServer("127.0.0.1:9092"),
    KafkaConfig.BootstrapServer("127.0.0.1:9093"),
    KafkaConfig.BootstrapServer("127.0.0.1:9094")
  )

  def create(topics: Topic*) = ApplicationConfig(
    kafka = KafkaConfig(
      bootstrapServers,
      KafkaConfig.SchemaRegistryUrl(
        url = "http://localhost:8081"
      ),
      topics.map { topic ⇒
        TopicConfig(
          name = topic,
          partitions = TopicConfig.Partitions(1),
          replicationFactor = TopicConfig.ReplicationFactor(1)
        )
      }.toList
    ),
    writeSide = WriteSideConfig(
      WriteSideConfig.UrlPath("system")
    )
  )

}
