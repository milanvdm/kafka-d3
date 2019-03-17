package me.milan.pubsub.kafka

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig }

import me.milan.config.KafkaConfig
import me.milan.domain.Key
import me.milan.serdes._

class KProducer(config: KafkaConfig) {

  private val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))
  producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, Key.generate.value)
  producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.url)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer)
  producerProps.put("value.subject.name.strategy", AvroSubjectStrategy)

  lazy val producer = new KafkaProducer[String, GenericRecord](
    producerProps
  )

}
