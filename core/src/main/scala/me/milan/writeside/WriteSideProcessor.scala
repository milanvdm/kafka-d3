package me.milan.writeside

import java.util.Properties

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.Sync
import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.processor.{ LogAndSkipOnInvalidTimestamp, Processor, ProcessorSupplier }
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{ QueryableStoreTypes, StreamsMetadata }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.http4s.Uri

import me.milan.config.KafkaConfig
import me.milan.domain.{ Aggregator, Done, Topic }
import me.milan.serdes.{ AvroSerde, KafkaAvroSerde, StringSerde }
import me.milan.writeside.kafka.{ KafkaProcessor, KafkaStore }

object WriteSideProcessor {

  def kafka[F[_], A >: Null: SchemaFor: Decoder: Encoder, E >: Null: SchemaFor: Decoder: Encoder](
    config: KafkaConfig,
    aggregator: Aggregator[A, E],
    name: String,
    from: Topic,
    to: Topic
  )(
    implicit
    S: Sync[F]
  ): WriteSideProcessor[F, A] = new KafkaWriteSideProcessor[F, A, E](config, aggregator, name, from, to)

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
  ): WriteSideProcessor[F, A] =
    new KafkaTimeToLiveWriteSideProcessor[F, A, E](config, aggregator, name, from, to, timeToLive)

}

trait WriteSideProcessor[F[_], A] {

  def start: F[Done]
  def aggregateById(key: String): F[Option[A]]
  def hosts: F[Set[Uri]]
  def partitionHost(key: String): F[Option[Uri]]
  def clean: F[Done]
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
  S: Sync[F]
) extends WriteSideProcessor[F, A] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))
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

  //TODO: Check if state is not running because stream needs to be stopped first
  override def start: F[Done] = S.delay {
    shutdownHook()
    stream = create // Stream object needs to be recreated on startup
    stream.start()
    Done.instance
  }

  override def aggregateById(key: String): F[Option[A]] = {
    val aggregateAvroSerde = new AvroSerde[A]
    val encodedAggregate = Option(
      stream
        .store(s"store-${from.value}-${to.value}", QueryableStoreTypes.keyValueStore[String, GenericRecord])
        .get(key)
    )

    S.delay(
      encodedAggregate.map(aggregateAvroSerde.decode)
    )
  }

  override def hosts: F[Set[Uri]] =
    S.delay(
      stream.allMetadata.asScala
        .map(metadata ⇒ Uri.fromString(s"${metadata.host}:${metadata.port}"))
        .collect {
          case Right(uri) ⇒ uri
        }
        .toSet
    )

  override def partitionHost(key: String): F[Option[Uri]] = {
    val metadata = stream.metadataForKey(s"store-${from.value}-${to.value}", key, Serdes.String.serializer)

    val uri = metadata match {
      case StreamsMetadata.NOT_AVAILABLE ⇒ None
      case _                             ⇒ Uri.fromString(s"${metadata.host}:${metadata.port}").toOption
    }

    S.delay(
      uri
    )
  }

  override def clean: F[Done] = S.delay {
    stream.cleanUp()
    Done.instance
  }

  override def stop: F[Done] = S.delay {
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
  S: Sync[F]
) extends WriteSideProcessor[F, A] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))
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

  //TODO: Check if state is not running because stream needs to be stopped first
  override def start: F[Done] = S.delay {
    shutdownHook()
    stream = create
    stream.start()
    Done.instance
  }

  override def aggregateById(key: String): F[Option[A]] = {
    val aggregateAvroSerde = new AvroSerde[A]
    val encodedAggregate = Option(
      stream
        .store(s"store-${from.value}-${to.value}", QueryableStoreTypes.keyValueStore[String, GenericRecord])
        .get(key)
    )

    S.delay(
      encodedAggregate.map(aggregateAvroSerde.decode)
    )
  }

  override def hosts: F[Set[Uri]] =
    S.delay(
      stream.allMetadata.asScala
        .map(metadata ⇒ Uri.fromString(s"${metadata.host}:${metadata.port}"))
        .collect {
          case Right(uri) ⇒ uri
        }
        .toSet
    )

  override def partitionHost(key: String): F[Option[Uri]] = {
    val metadata = stream.metadataForKey(s"store-${from.value}-${to.value}", key, Serdes.String.serializer)

    val uri = metadata match {
      case StreamsMetadata.NOT_AVAILABLE ⇒ None
      case _                             ⇒ Uri.fromString(s"${metadata.host}:${metadata.port}").toOption
    }

    S.delay(
      uri
    )
  }

  override def clean: F[Done] = S.delay {
    stream.cleanUp()
    Done.instance
  }

  override def stop: F[Done] = S.delay {
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
