package me.milan.commands

import scala.concurrent.duration.FiniteDuration

import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.{ Concurrent, Resource, Timer }
import cats.instances.either._
import cats.syntax.applicative._
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._

import me.milan.domain.{ Command, CommandResponse, Error, Key, Record }
import me.milan.pubsub.{ Pub, Sub }

object CommandCoordinator {

  def kafka[F[_]: Concurrent: Timer, C, R](
    timeOut: FiniteDuration,
    pub: Pub[F, Command[C]],
    sub: Sub[F, CommandResponse[R]]
  ): Resource[F, CommandCoordinator[F, C, R]] =
    for {
      promises <- Resource.liftF(Ref.of(Map.empty[Key, Deferred[F, R]]))
      processor <- KafkaCommandCoordinator(timeOut, pub, sub, promises).start
    } yield processor

}

trait CommandCoordinator[F[_], C, R] {

  def send(command: Record[Command[C]]): F[Error Either R]

}

private[commands] case class KafkaCommandCoordinator[F[_]: Concurrent: Timer, C, R](
  timeOut: FiniteDuration,
  pub: Pub[F, Command[C]],
  sub: Sub[F, CommandResponse[R]],
  promises: Ref[F, Map[Key, Deferred[F, R]]]
) extends CommandCoordinator[F, C, R] {

  def start: Resource[F, KafkaCommandCoordinator[F, C, R]] =
    sub.start
      .map { record =>
        promises.get
          .map(_.get(record.value.id).map(_.complete(record.value.response)))
          .void
      }
      .compile
      .resource
      .drain
      .map(_ => this)

  override def send(command: Record[Command[C]]): F[Error Either R] =
    for {
      defer <- Deferred[F, R]
      _ <- promises.update(_.updated(command.value.id, defer))
      _ <- pub.publish(command)
      response <- Concurrent.timeoutTo(
        defer.get.map(_.asRight[Error]),
        timeOut,
        Error.TimeOut.asLeft[R].leftWiden[Error].pure[F]
      )
      _ <- promises.update(_ - command.value.id)
    } yield response

}
