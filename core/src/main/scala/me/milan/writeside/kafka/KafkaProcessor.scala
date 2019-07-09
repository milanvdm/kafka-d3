package me.milan.writeside.kafka

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import com.sksamuel.avro4s.{ Decoder, Encoder, SchemaFor }
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.{ Processor, ProcessorContext }
import org.apache.kafka.streams.state.KeyValueStore

import me.milan.domain.Aggregator
import me.milan.serdes.{ AvroSerde, TimedGenericRecord }

object KafkaProcessor {

  def aggregate[A >: Null: SchemaFor: Decoder: Encoder, E >: Null: SchemaFor: Decoder: Encoder](
    storeName: String,
    aggregator: Aggregator[A, E]
  ): Processor[String, GenericRecord] = new AggregateKafkaProcessor[A, E](storeName, aggregator)

  def ttlAggregate[A >: Null: SchemaFor: Decoder: Encoder, E >: Null: SchemaFor: Decoder: Encoder](
    storeName: String,
    aggregator: Aggregator[A, E],
    timeToLive: FiniteDuration
  ): Processor[String, GenericRecord] =
    new TimeToLiveAggregateKafkaProcessor[A, E](storeName, aggregator, timeToLive)

}

private[kafka] class AggregateKafkaProcessor[
  A >: Null: SchemaFor: Decoder: Encoder,
  E >: Null: SchemaFor: Decoder: Encoder
](
  storeName: String,
  aggregator: Aggregator[A, E]
) extends Processor[String, GenericRecord] {

  private var processorContext: ProcessorContext = _
  private var kvStore: KeyValueStore[String, GenericRecord] = _

  private val eventAvroSerde = new AvroSerde[E]
  private val aggregateAvroSerde = new AvroSerde[A]

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    Try {
      this.kvStore = processorContext
        .getStateStore(storeName)
        .asInstanceOf[KeyValueStore[String, GenericRecord]]
    } match {
      case Success(_) => ()
      case Failure(e: ClassCastException) =>
        throw new IllegalArgumentException(s"Please provide a KeyValueStore, reason: $e")
      case Failure(e) => throw e
    }
  }

  override def process(
    key: String,
    event: GenericRecord
  ): Unit = {
    val decodedEvent = eventAvroSerde.decode(event)
    val currentAggregate = Option(kvStore.get(key))
    val decodedCurrentAggregate = currentAggregate.map(aggregateAvroSerde.decode)
    val newAggregate = aggregator.process(decodedCurrentAggregate, decodedEvent)
    val encodedNewAggregate = aggregateAvroSerde.encode(newAggregate)

    kvStore.put(key, encodedNewAggregate)
    processorContext.forward(key, encodedNewAggregate)
    processorContext.commit()
  }

  override def close(): Unit = ()
}

private[kafka] class TimeToLiveAggregateKafkaProcessor[
  A >: Null: SchemaFor: Decoder: Encoder,
  E >: Null: SchemaFor: Decoder: Encoder
](
  storeName: String,
  aggregator: Aggregator[A, E],
  timeToLive: FiniteDuration
) extends Processor[String, GenericRecord] {

  private var processorContext: ProcessorContext = _
  private var kvStore: KeyValueStore[String, TimedGenericRecord] = _

  private val eventAvroSerde = new AvroSerde[E]
  private val aggregateAvroSerde = new AvroSerde[A]

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    Try {
      this.kvStore = processorContext
        .getStateStore(storeName)
        .asInstanceOf[KeyValueStore[String, TimedGenericRecord]]
    } match {
      case Success(_) => ()
      case Failure(e: ClassCastException) =>
        throw new IllegalArgumentException(s"Please provide a KeyValueStore, reason: $e")
      case Failure(e) => throw e
    }
  }

  override def process(
    key: String,
    event: GenericRecord
  ): Unit = {
    val timedGenericRecord = Option(kvStore.get(key))

    val timedOut = timedGenericRecord match {
      case Some(TimedGenericRecord(_, timestamp)) =>
        val timeDifference = (timestamp - processorContext.timestamp).millis
        processorContext.timestamp < timestamp && timeToLive < timeDifference
      case None => false
    }

    if (timedOut) {
      return
    }

    val decodedEvent = eventAvroSerde.decode(event)
    val currentAggregate = timedGenericRecord.map(_.record)
    val decodedCurrentAggregate = currentAggregate.map(aggregateAvroSerde.decode)
    val newAggregate = aggregator.process(decodedCurrentAggregate, decodedEvent)
    val encodedNewAggregate = TimedGenericRecord(
      aggregateAvroSerde.encode(newAggregate),
      Math.max(processorContext.timestamp, timedGenericRecord.map(_.timestamp).getOrElse(-1L))
    )

    kvStore.put(key, encodedNewAggregate)
    processorContext.forward(key, encodedNewAggregate.record)
    processorContext.commit()
  }

  override def close(): Unit = ()

}
