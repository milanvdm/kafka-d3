package me.milan.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import cats.Show
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.http4s.Uri

import me.milan.config.AggregateStoreConfig.UrlPath
import me.milan.domain.Topic

case class ApplicationConfig(
  kafka: KafkaConfig,
  aggregateStore: AggregateStoreConfig
)

case class KafkaConfig(
  bootstrapServers: Set[KafkaConfig.BootstrapServer],
  schemaRegistry: KafkaConfig.SchemaRegistryConfig,
  topics: List[KafkaConfig.TopicConfig],
  consumer: KafkaConfig.ConsumerConfig = KafkaConfig.ConsumerConfig()
)

object KafkaConfig {

  case class BootstrapServer(uri: Uri) extends AnyVal
  object BootstrapServer {
    implicit val bootstrapServerShow: Show[BootstrapServer] = Show.show(_.uri.renderString)
  }
  case class SchemaRegistryConfig(
    uri: Uri,
    identityMapCapacity: Int = 1000
  )

  case class TopicConfig(
    name: Topic,
    partitions: TopicConfig.Partitions,
    replicationFactor: TopicConfig.ReplicationFactor,
    retention: FiniteDuration = FiniteDuration(Long.MaxValue, TimeUnit.NANOSECONDS)
  )

  object TopicConfig {
    case class Partitions(value: Int) extends AnyVal
    case class ReplicationFactor(value: Int) extends AnyVal
  }

  case class ConsumerConfig(maxPollInterval: FiniteDuration = FiniteDuration(Int.MaxValue, TimeUnit.MILLISECONDS))

}

case class AggregateStoreConfig(
  urlPath: UrlPath,
  autoOffsetReset: AutoOffsetReset = AutoOffsetReset.EARLIEST
)

object AggregateStoreConfig {
  case class UrlPath(value: String) extends AnyVal

  implicit val autoOffsetResetShow: Show[AutoOffsetReset] = Show.show(_.toString.toLowerCase)
}
