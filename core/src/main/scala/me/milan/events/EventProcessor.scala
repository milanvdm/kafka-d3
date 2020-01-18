package me.milan.events

import java.util.Properties

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.{ Concurrent, Sync }
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import fs2.concurrent.SignallingRef
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.processor.{ LogAndSkipOnInvalidTimestamp, Processor, ProcessorSupplier }
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }

import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.config.AggregateStoreConfig._
import me.milan.config.{ AggregateStoreConfig, KafkaConfig }
import me.milan.discovery.Registry
import me.milan.discovery.Registry.RegistryGroup
import me.milan.domain.{ Aggregator, Topic }
import me.milan.events.kafka.{ KafkaProcessor, ProcessorName }
import me.milan.serdes.{ AvroSerde, KafkaAvroSerde, StringSerde }
import me.milan.store.kafka.{ KafkaStore, StoreName }

object EventProcessor {

  def kafka[F[_]: Concurrent, A >: Null: AvroSerde, E >: Null: AvroSerde](
    kafkaConfig: KafkaConfig,
    aggregateStoreConfig: AggregateStoreConfig,
    registry: Registry[F],
    aggregator: Aggregator[A, E],
    name: String,
    from: Topic,
    to: Topic,
    timeToLive: Option[FiniteDuration] = None
  ): F[EventProcessor[F]] =
    SignallingRef[F, Boolean](false).map { haltSignal =>
      new KafkaEventProcessor[F, A, E](
        kafkaConfig,
        aggregateStoreConfig,
        registry,
        aggregator,
        name,
        from,
        to,
        timeToLive,
        haltSignal
      )
    }

}

trait EventProcessor[F[_]] {

  def start: F[Unit]
  def clean: F[Unit]
  def stop: F[Unit]

}

class KafkaEventProcessor[F[_]: Concurrent, A >: Null: AvroSerde, E >: Null: AvroSerde](
  kafkaConfig: KafkaConfig,
  aggregateStoreConfig: AggregateStoreConfig,
  registry: Registry[F],
  aggregator: Aggregator[A, E],
  name: String,
  from: Topic,
  to: Topic,
  timeToLive: Option[FiniteDuration],
  haltSignal: SignallingRef[F, Boolean]
) extends EventProcessor[F] {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers.map(_.show).mkString(","))
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.schemaRegistry.uri.renderString)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, aggregateStoreConfig.autoOffsetReset.show)
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)
    p
  }

  val storeName = StoreName(from, to)
  val processorName = ProcessorName(from, to)
  val registryGroup = RegistryGroup(s"${processorName.value}-stream")

  var stream: KafkaStreams = _

  //TODO: Check if state is not running because stream needs to be stopped first
  override def start: F[Unit] =
    for {
      _ <- haltSignal.set(false)
      _ <- Sync[F].delay {
        shutdownHook()
        stream = create
        stream.start()
      }
      _ <- registry
        .register(registryGroup)
        .interruptWhen(haltSignal)
        .compile
        .drain
    } yield ()

  override def clean: F[Unit] =
    Sync[F]
      .delay(stream.cleanUp())

  override def stop: F[Unit] =
    for {
      _ <- Sync[F].delay(stream.close(10.seconds.toJava))
      _ <- haltSignal.set(true)
    } yield ()

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
