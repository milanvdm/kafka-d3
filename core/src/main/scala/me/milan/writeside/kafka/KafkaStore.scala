package me.milan.writeside.kafka

import scala.collection.JavaConverters._

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }

import me.milan.config.KafkaConfig.SchemaRegistryUrl
import me.milan.serdes.TimedGenericRecord

object KafkaStore {

  def kvStoreBuilder(
    schemaRegistryConfig: SchemaRegistryUrl,
    storeName: String
  ): StoreBuilder[KeyValueStore[String, GenericRecord]] = {
    val serdeConfig = Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG → schemaRegistryConfig.url
    )

    val valueGenericAvroSerde = new GenericAvroSerde()
    valueGenericAvroSerde.configure(serdeConfig.asJava, false)

    Stores
      .keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        Serdes.String,
        valueGenericAvroSerde
      )
      .withCachingDisabled
  }

  def kvWithTimeStoreBuilder(
    schemaRegistryConfig: SchemaRegistryUrl,
    storeName: String
  ): StoreBuilder[KeyValueStore[String, TimedGenericRecord]] = {
    val serdeConfig = Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG → schemaRegistryConfig.url
    )

    val genericAvroSerde = new GenericAvroSerde()
    genericAvroSerde.configure(serdeConfig.asJava, false)

    val timedGenericRecordSerdes = TimedGenericRecord.serdes(
      genericAvroSerde,
      Serdes.Long
    )

    Stores
      .keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        Serdes.String,
        timedGenericRecordSerdes
      )
      .withCachingDisabled
  }
}
