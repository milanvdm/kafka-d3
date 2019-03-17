package me.milan.pubsub.kafka

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }

import me.milan.config.KafkaConfig
import me.milan.serdes._

object KConsumer {
  case class ConsumerGroupId(value: String) extends AnyVal
}

class KConsumer(config: KafkaConfig) {

  val consumerProps: Properties = new Properties()
  // TODO: different consumers in the same group, will consume different partitions and not get all the state changes across partitions!
  consumerProps.put("group.id", "example-consumer-group")
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))
  consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.url)
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer)
  consumerProps.put("value.subject.name.strategy", AvroSubjectStrategy)

  lazy val consumer = new KafkaConsumer[String, GenericRecord](
    consumerProps
  )

}
