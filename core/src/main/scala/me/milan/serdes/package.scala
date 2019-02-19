package me.milan

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.streams.scala.Serdes

package object serdes {

  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"
  val StringDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
  val StringSerde: String = Serdes.String.getClass.getName

  val KafkaAvroSerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer"
  val KafkaAvroDeserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
  val KafkaAvroSerde: String = new GenericAvroSerde().getClass.getName

  val AvroSubjectStrategy = "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"

}
