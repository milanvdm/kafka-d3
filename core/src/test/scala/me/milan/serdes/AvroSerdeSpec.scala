package me.milan.serdes

import com.sksamuel.avro4s.RecordFormat
import org.scalatest.{Matchers, WordSpec}

import me.milan.domain.TombStone

class AvroSerdeSpec extends WordSpec with Matchers {
  import AvroSerdeSpec._

  "AvroSerde" can {

    "encode and decode" should {

      "successfully give back the same object" in {

        val avroSerde = new AvroSerde[Key1]

        val result = avroSerde.decode(avroSerde.encode(key1))

        result shouldBe key1

      }

      "successfully decode to the right sub type of sealed trait" in {

        val subtypeFormat = RecordFormat[Key1]

        val record = subtypeFormat.to(key1)

        val traitFormat = RecordFormat[Key]

        val result = traitFormat.from(record)

       result shouldBe key1

      }

      "successfully encode a Tombstone type" in {

        val avroSerde = new AvroSerde[Key3]

        val result = avroSerde.encode(key3)

        result shouldBe null

      }

      "fail on non case classes" in {

        val avroSerde = new AvroSerde[String]

        val thrown = the[java.lang.RuntimeException] thrownBy {
            avroSerde.decode(avroSerde.encode(testString))
          }

        thrown.getMessage shouldBe "Cannot marshall an instance of test to a Record (was test)"

      }
    }
  }

}

object AvroSerdeSpec {

  sealed trait Key
  case class Wrapper(wrapped: Key)
  case class Key1(value: String) extends Key
  case class Key2(value: String) extends Key
  case class Key3(value: String) extends Key with TombStone

  val key1 = Key1("key1")
  val key2 = Key2("key2")
  val key3 = Key3("key3")

  val testString = "test"

}
