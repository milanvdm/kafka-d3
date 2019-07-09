package me.milan.commands

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._

import me.milan.pubsub.Pub

object CommandProcessor {

  def dummy[F[_], V](
    pub: Pub[F, V]
  )(
    implicit
    M: Monad[F]
  ): CommandProcessor[F, V] = DummyCommandProcessor(pub)

}

trait CommandProcessor[F[_], V] {

  def process(command: Command): F[Unit]

}

private[commands] case class DummyCommandProcessor[F[_], V](
  pub: Pub[F, V]
)(
  implicit
  M: Monad[F]
) extends CommandProcessor[F, V] {

  override def process(command: Command): F[Unit] =
    for {
      event <- command.verify[F, V]
      result <- pub.publish(event)
    } yield result

}
