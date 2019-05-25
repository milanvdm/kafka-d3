package me.milan.pubsub.kafka

import java.util.Properties

import cats.instances.set._
import cats.instances.finiteDuration._
import cats.syntax.show._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId
import me.milan.serdes._

object KConsumer {
  case class ConsumerGroupId(value: String) extends AnyVal
}

class KConsumer(
  config: KafkaConfig,
  consumerGroupId: ConsumerGroupId
) {

  val consumerProps: Properties = new Properties()
  consumerProps.put("group.id", consumerGroupId.value)
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.show)
  consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.url.renderString)
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer)
  consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.consumer.maxPollInterval.show)
  consumerProps.put("value.subject.name.strategy", AvroSubjectStrategy)

  lazy val consumer = new KafkaConsumer[String, GenericRecord](
    consumerProps
  )

}
