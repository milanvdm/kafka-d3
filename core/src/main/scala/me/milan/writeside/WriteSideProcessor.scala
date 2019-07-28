package me.milan.writeside

import java.util.Properties

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.Sync
import cats.syntax.show._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.processor.{ LogAndSkipOnInvalidTimestamp, Processor, ProcessorSupplier }
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{ QueryableStoreTypes, StreamsMetadata }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.http4s.Uri

import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.config.WriteSideConfig._
import me.milan.config.{ KafkaConfig, WriteSideConfig }
import me.milan.domain.{ Aggregator, Topic }
import me.milan.serdes.{ AvroSerde, KafkaAvroSerde, StringSerde }
import me.milan.writeside.kafka.{ KafkaProcessor, KafkaStore, ProcessorName, StoreName }

object WriteSideProcessor {

  def kafka[F[_]: Sync, A >: Null: AvroSerde, E >: Null: AvroSerde](
    kafkaConfig: KafkaConfig,
    writeSideConfig: WriteSideConfig,
    aggregator: Aggregator[A, E],
    name: String,
    from: Topic,
    to: Topic,
    timeToLive: Option[FiniteDuration] = None
  ): WriteSideProcessor[F, A] =
    new KafkaWriteSideProcessor[F, A, E](kafkaConfig, writeSideConfig, aggregator, name, from, to, timeToLive)

}

trait WriteSideProcessor[F[_], A] {

  def start: F[Unit]
  def aggregateById(key: String): F[Option[A]]
  def hosts: F[Set[Uri]]
  def partitionHost(key: String): F[Option[Uri]]
  def clean: F[Unit]
  def stop: F[Unit]

}

private[writeside] class KafkaWriteSideProcessor[F[_]: Sync, A >: Null: AvroSerde, E >: Null: AvroSerde](
  kafkaConfig: KafkaConfig,
  writeSideConfig: WriteSideConfig,
  aggregator: Aggregator[A, E],
  name: String,
  from: Topic,
  to: Topic,
  timeToLive: Option[FiniteDuration]
) extends WriteSideProcessor[F, A] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers.map(_.show).mkString(","))
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.schemaRegistry.uri.renderString)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, writeSideConfig.autoOffsetReset.show)
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)
    p
  }

  val storeName = StoreName(from, to)
  val processorName = ProcessorName(from, to)

  var stream: KafkaStreams = _

  //TODO: Check if state is not running because stream needs to be stopped first
  override def start: F[Unit] = Sync[F].delay {
    shutdownHook()
    stream = create
    stream.start()
  }

  override def aggregateById(key: String): F[Option[A]] = {
    val encodedAggregate = Option(
      stream
        .store(storeName.value, QueryableStoreTypes.keyValueStore[String, GenericRecord])
        .get(key)
    )

    Sync[F].delay(
      encodedAggregate.map(AvroSerde[A].decode)
    )
  }

  override def hosts: F[Set[Uri]] =
    Sync[F].delay(
      stream.allMetadata.asScala
        .map(metadata => Uri.fromString(s"${metadata.host}:${metadata.port}"))
        .collect {
          case Right(uri) => uri
        }
        .toSet
    )

  override def partitionHost(key: String): F[Option[Uri]] = {
    val metadata = stream.metadataForKey(storeName.value, key, Serdes.String.serializer)

    val uri = metadata match {
      case StreamsMetadata.NOT_AVAILABLE => None
      case _                             => Uri.fromString(s"${metadata.host}:${metadata.port}").toOption
    }

    Sync[F].delay(
      uri
    )
  }

  override def clean: F[Unit] = Sync[F].delay {
    stream.cleanUp()
  }

  override def stop: F[Unit] = Sync[F].delay {
    stream.close(10.seconds.toJava)
    ()
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
        processorName.value,
        new ProcessorSupplier[String, GenericRecord] {
          override def get(): Processor[String, GenericRecord] =
            KafkaProcessor.aggregate(
              storeName,
              aggregator,
              timeToLive
            )
        },
        s"source-${from.value}"
      )
      .addStateStore(
        KafkaStore.kvStoreBuilder(kafkaConfig.schemaRegistry, storeName),
        processorName.value
      )
      .addSink(s"sink-${to.value}", to.value, processorName.value)

    new KafkaStreams(builder, props)
  }

  private def shutdownHook(): Unit =
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        stream.close()
    })
}
