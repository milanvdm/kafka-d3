package me.milan.writeside.kafka

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.{ Processor, ProcessorContext }
import org.apache.kafka.streams.state.{ TimestampedKeyValueStore, ValueAndTimestamp }

import me.milan.domain.{ Aggregator, Topic }
import me.milan.serdes.AvroSerde

case class ProcessorName(value: String) extends AnyVal
object ProcessorName {
  def apply(
    from: Topic,
    to: Topic
  ): StoreName = new StoreName(s"processor-${from.value}-${to.value}")
}

object KafkaProcessor {

  def aggregate[A >: Null: AvroSerde, E >: Null: AvroSerde](
    storeName: StoreName,
    aggregator: Aggregator[A, E],
    timeToLive: Option[FiniteDuration]
  ): Processor[String, GenericRecord] = new AggregateKafkaProcessor[A, E](storeName, aggregator, timeToLive)

}

private[kafka] class AggregateKafkaProcessor[A >: Null: AvroSerde, E >: Null: AvroSerde](
  storeName: StoreName,
  aggregator: Aggregator[A, E],
  timeToLive: Option[FiniteDuration]
) extends Processor[String, GenericRecord] {

  private var processorContext: ProcessorContext = _
  private var kvStore: TimestampedKeyValueStore[String, GenericRecord] = _

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    Try {
      this.kvStore = processorContext
        .getStateStore(storeName.value)
        .asInstanceOf[TimestampedKeyValueStore[String, GenericRecord]]
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

    val onTime = timeToLive.map { ttl =>
      timedGenericRecord.exists { timeAndValue =>
        val timeDifference = (timeAndValue.timestamp - processorContext.timestamp).millis
        processorContext.timestamp > timeAndValue.timestamp || ttl > timeDifference
      }
    }

    if (onTime.forall(!_)) {
      return
    }

    val decodedEvent = AvroSerde[E].decode(event)
    val currentAggregate = timedGenericRecord.map(_.value)
    val decodedCurrentAggregate = currentAggregate.map(AvroSerde[A].decode)
    val newAggregate = aggregator.process(decodedCurrentAggregate, decodedEvent)
    val encodedNewAggregate = ValueAndTimestamp.make[GenericRecord](
      AvroSerde[A].encode(newAggregate),
      Math.max(processorContext.timestamp, timedGenericRecord.map(_.timestamp).getOrElse(-1L))
    )

    kvStore.put(key, encodedNewAggregate)
    processorContext.forward(key, encodedNewAggregate)
    processorContext.commit()
  }

  override def close(): Unit = ()

}
