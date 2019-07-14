package me.milan.kafka

import me.milan.domain.{ Key, Record, Topic }
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId

class Fixtures {
  import Fixtures._

  val consumerGroupId = ConsumerGroupId(Key.generate.value)
  val topic = Topic(Key.generate.value)

  val from = Topic(Key.generate.value)
  val to = Topic(Key.generate.value)

  val systemTopic = Topic("_system")

  val key1 = Key("key1")
  val key2 = Key("key2")
  val key3 = Key("key3")
  val value1 = Value1("value1")
  val value2 = Value2("value2")
  val value3 = Value3("value3")

  val record1: Record[Value1] = Record(topic, key1, value1, 100L)
  val record2: Record[Value2] = Record(topic, key2, value2, 200L)
  val record3: Record[Value3] = Record(topic, key3, value3, 300L)

}

object Fixtures {

  sealed trait Value
  case class Value1(value1: String) extends Value
  case class Value2(value2: String) extends Value
  case class Value3(value3: String) extends Value

}
