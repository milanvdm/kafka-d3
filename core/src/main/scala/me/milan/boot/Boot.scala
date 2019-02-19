package me.milan.boot

object Boot extends App {

  trait WithDecoder[T] {
    def decode(g: Generic): Option[T]
  }

  case class Generic(value: String)

  case class Specific1(value: String)
  object Specific1 {

    implicit val decoder: WithDecoder[Specific1] = new WithDecoder[Specific1] {
      override def decode(g: Generic): Option[Specific1] = Some(Specific1(g.value))
    }

  }

  case class Specific2(value: Int)
  case class Specific3(value: Double)

  object GenericMatcher {

    def check[F](g: Generic)(implicit decoder: WithDecoder[F]): Option[F] =
      decoder.decode(g)

  }

  val generic = Generic("test")

  GenericMatcher.check[Specific1](generic)

}
