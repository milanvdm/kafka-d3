package me.milan.config

import org.http4s.Uri

import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.Topic

object TestConfig {

  val bootstrapServers: Set[KafkaConfig.BootstrapServer] =
    Set(
      KafkaConfig.BootstrapServer(Uri.unsafeFromString("http://localhost:9092")),
      KafkaConfig.BootstrapServer(Uri.unsafeFromString("http://localhost:9093")),
      KafkaConfig.BootstrapServer(Uri.unsafeFromString("http://localhost:9094"))
    )

  def create(topics: Topic*) = ApplicationConfig(
    kafka = KafkaConfig(
      bootstrapServers,
      KafkaConfig.SchemaRegistryConfig(
        uri = Uri.unsafeFromString("http://localhost:8081")
      ),
      topics.map { topic =>
        TopicConfig(
          name = topic,
          partitions = TopicConfig.Partitions(3),
          replicationFactor = TopicConfig.ReplicationFactor(3)
        )
      }.toList
    ),
    writeSide = AggregateStoreConfig(
      AggregateStoreConfig.UrlPath("system")
    )
  )

}
