package me.milan.store.kafka

import scala.jdk.CollectionConverters._

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{ StoreBuilder, Stores, TimestampedKeyValueStore }

import me.milan.config.KafkaConfig.SchemaRegistryConfig
import me.milan.domain.Topic

case class StoreName(value: String) extends AnyVal
object StoreName {
  def apply(
    from: Topic,
    to: Topic
  ): StoreName = new StoreName(s"store-${from.value}-${to.value}")
}

object KafkaStore {

  def kvStoreBuilder(
    schemaRegistryConfig: SchemaRegistryConfig,
    storeName: StoreName
  ): StoreBuilder[TimestampedKeyValueStore[String, GenericRecord]] =
    Stores
      .timestampedKeyValueStoreBuilder[String, GenericRecord](
        Stores.persistentKeyValueStore(storeName.value),
        Serdes.String,
        genericRecordAvroSerde(schemaRegistryConfig)
      )
      .withCachingDisabled

  private def genericRecordAvroSerde(schemaRegistryConfig: SchemaRegistryConfig): GenericAvroSerde = {
    val serdeConfig = Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryConfig.uri.renderString
    )

    val genericAvroSerde = new GenericAvroSerde()
    genericAvroSerde.configure(serdeConfig.asJava, false)
    genericAvroSerde
  }
}
