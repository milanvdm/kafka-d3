package me.example.domain

import cats.effect.Effect
import me.milan.domain.{Key, Record}

object UserCommands {

  sealed trait UserEvent extends Product with Serializable
  case class CreateUser(
                          name: String
                        ) extends Command {

    override def verify[F[_]: Effect, V]: F[Record[V]] = ???

  }

  case class UpdateUser(
                          id: Key,
                          name: String
                        ) extends Command {

    override def verify[F[_]: Effect, V]: F[Record[V]] = ???

  }

}
