package me.milan.config

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.Topic

object Config {

  def create(topics: Topic*) = ApplicationConfig(
    kafka = KafkaConfig(
      KafkaConfig.BootstrapServer("localhost:9092"),
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
