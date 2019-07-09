package me.milan.pubsub.kafka

import java.util.Properties

import cats.effect.Sync
import cats.instances.long._
import cats.syntax.show._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.serdes._

object KConsumer {
  case class ConsumerGroupId(value: String) extends AnyVal

  def apply[F[_]: Sync](
    config: KafkaConfig,
    consumerGroupId: ConsumerGroupId
  ): F[KConsumer] = Sync[F].delay {
    val consumerProps: Properties = new Properties()
    consumerProps.put("group.id", consumerGroupId.value)
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.show).mkString(","))
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.uri.renderString)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer)
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.consumer.maxPollInterval.toMillis.show)
    consumerProps.put("value.subject.name.strategy", AvroSubjectStrategy)

    KConsumer(
      new KafkaConsumer[String, GenericRecord](
        consumerProps
      )
    )
  }
}

case class KConsumer(consumer: KafkaConsumer[String, GenericRecord])
