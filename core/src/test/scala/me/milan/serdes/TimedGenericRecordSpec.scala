package me.milan.serdes

import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{Matchers, WordSpec}

class TimedGenericRecordSpec extends WordSpec with Matchers {
  import TimedGenericRecordSpec._

  "TimedGenericRecord" can {

    "encode and decode a Long" should {

      "successfully give back the same number" in {

        val bytes = Serdes.Long.serializer.serialize("test", timestamp)
        val result = Serdes.Long.deserializer.deserialize(
          "test",
          bytes
        )

        bytes should have size 8
        result shouldBe timestamp

      }
    }
  }

}

object TimedGenericRecordSpec {

  val timestamp = 1L

}
