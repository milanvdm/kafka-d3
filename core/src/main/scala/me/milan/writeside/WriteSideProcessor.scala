package me.milan.writeside

import java.util.Properties

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.Sync
import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.processor.{ LogAndSkipOnInvalidTimestamp, Processor, ProcessorSupplier }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }

import me.milan.config.KafkaConfig
import me.milan.domain.{ Aggregator, Done, Topic }
import me.milan.serdes.{ AvroSerde, KafkaAvroSerde, StringSerde }
import me.milan.writeside.kafka.{ KafkaProcessor, KafkaStore }

object WriteSideProcessor {

  //  def dummy[F[_]](
  //                   implicit _F:
  //  Functor[F]
  //                 ): WriteSideProcessor[F] = DummyWriteSideProcessor()

  def kafka[F[_], A >: Null: SchemaFor: Decoder: Encoder, E >: Null: SchemaFor: Decoder: Encoder](
    config: KafkaConfig,
    aggregator: Aggregator[A, E],
    name: String,
    from: Topic,
    to: Topic
  )(
    implicit
    F: Sync[F]
  ): WriteSideProcessor[F] = new KafkaWriteSideProcessor[F, A, E](config, aggregator, name, from, to)

  def kafkaTimeToLive[F[_], A >: Null: SchemaFor: Decoder: Encoder, E >: Null: SchemaFor: Decoder: Encoder](
    config: KafkaConfig,
    aggregator: Aggregator[A, E],
    name: String,
    from: Topic,
    to: Topic,
    timeToLive: FiniteDuration
  )(
    implicit
    F: Sync[F]
  ): WriteSideProcessor[F] =
    new KafkaTimeToLiveWriteSideProcessor[F, A, E](config, aggregator, name, from, to, timeToLive)

}

trait WriteSideProcessor[F[_]] {

  def start: F[Done]
  //def aggregateById[A](key: String): F[A]
  def stop: F[Done]

}

private[writeside] class KafkaWriteSideProcessor[
  F[_],
  A >: Null: SchemaFor: Decoder: Encoder,
  E >: Null: SchemaFor: Decoder: Encoder
](
  config: KafkaConfig,
  aggregator: Aggregator[A, E],
  name: String,
  from: Topic,
  to: Topic
)(
  implicit
  F: Sync[F]
) extends WriteSideProcessor[F] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer.value)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.url)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)
    p
  }

  val eventAvroSerde = new AvroSerde[E]
  val aggregateAvroSerde = new AvroSerde[A]

  var stream: KafkaStreams = _

  override def start: F[Done] = F.delay {
    stream = create
    shutdownHook()
    stream.start()
    Done.instance
  }

  override def stop: F[Done] = F.delay {
    stream.close(10.seconds.toJava)
    Done.instance
  }

  private def create: KafkaStreams = {
    val builder: Topology = new Topology

    builder
      .addSource(s"source-${from.value}", from.value)
      .addProcessor(
        s"process-${from.value}-${to.value}",
        new ProcessorSupplier[String, GenericRecord] {
          override def get(): Processor[String, GenericRecord] =
            KafkaProcessor.aggregate(s"store-${from.value}-${to.value}", aggregator)
        },
        s"source-${from.value}"
      )
      .addStateStore(
        KafkaStore.kvStoreBuilder(config.schemaRegistry, s"store-${from.value}-${to.value}"),
        s"process-${from.value}-${to.value}"
      )
      .addSink(s"sink-${to.value}", to.value, s"process-${from.value}-${to.value}")

    new KafkaStreams(builder, props)
  }

  private def shutdownHook(): Unit =
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        stream.close()
    })
}

private[writeside] class KafkaTimeToLiveWriteSideProcessor[
  F[_],
  A >: Null: SchemaFor: Decoder: Encoder,
  E >: Null: SchemaFor: Decoder: Encoder
](
  config: KafkaConfig,
  aggregator: Aggregator[A, E],
  name: String,
  from: Topic,
  to: Topic,
  timeToLive: FiniteDuration
)(
  implicit
  F: Sync[F]
) extends WriteSideProcessor[F] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer.value)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistry.url)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)
    p
  }

  val eventAvroSerde = new AvroSerde[E]
  val aggregateAvroSerde = new AvroSerde[A]

  var stream: KafkaStreams = _

  override def start: F[Done] = F.delay {
    stream = create
    shutdownHook()
    stream.start()
    Done.instance
  }

  override def stop: F[Done] = F.delay {
    stream.close(10.seconds.toJava)
    Done.instance
  }

  private def create: KafkaStreams = {
    val builder: Topology = new Topology

    builder
      .addSource(
        new LogAndSkipOnInvalidTimestamp,
        s"source-${from.value}",
        from.value
      )
      .addProcessor(
        s"process-${from.value}-${to.value}",
        new ProcessorSupplier[String, GenericRecord] {
          override def get(): Processor[String, GenericRecord] =
            KafkaProcessor.ttlAggregate(
              s"store-${from.value}-${to.value}",
              aggregator,
              timeToLive
            )
        },
        s"source-${from.value}"
      )
      .addStateStore(
        KafkaStore.kvWithTimeStoreBuilder(config.schemaRegistry, s"store-${from.value}-${to.value}"),
        s"process-${from.value}-${to.value}"
      )
      .addSink(s"sink-${to.value}", to.value, s"process-${from.value}-${to.value}")

    new KafkaStreams(builder, props)
  }

  private def shutdownHook(): Unit =
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        stream.close()
    })
}

//private[me.milan.writeside] case class DummyWriteSideProcessor[F[_]]()(implicit _F: Functor[F]) extends WriteSideProcessor[F] {
//
//  override def start = ???
//  override def stop = ???
//
//}
