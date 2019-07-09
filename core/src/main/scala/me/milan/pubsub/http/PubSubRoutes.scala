package me.milan.pubsub.http

import cats.effect.Sync
import cats.syntax.apply._
import io.circe.Encoder
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import me.milan.config.WriteSideConfig
import me.milan.writeside.WriteSideProcessor
import me.milan.writeside.http.WriteSide

object PubSubRoutes {

  def http4sRoutes[F[_], A](
    writeSideConfig: WriteSideConfig,
    writeSide: WriteSide[F, A],
    writeSideProcessor: WriteSideProcessor[F, A]
  )(
    implicit
    encoder: Encoder[A],
    S: Sync[F]
  ): HttpRoutes[F] =
    new Http4sPubSubService[F, A](
      writeSideConfig,
      writeSide,
      writeSideProcessor
    ).routes

}

private[http] class Http4sPubSubService[F[_], A](
  writeSideConfig: WriteSideConfig,
  writeSide: WriteSide[F, A],
  writeSideProcessor: WriteSideProcessor[F, A]
)(
  implicit
  encoder: Encoder[A],
  S: Sync[F]
) extends Http4sDsl[F] {

  def routes: HttpRoutes[F] =
    HttpRoutes
      .of[F] {
        case POST -> (Root / "start" / writeSideConfig.urlPath.value / "start") =>
          writeSide.start.compile.drain *> Ok()
        case POST -> (Root / writeSideConfig.urlPath.value / "local" / "start") =>
          writeSideProcessor.start *> Ok()
        case GET -> (Root / writeSideConfig.urlPath.value / UUIDVar(key)) =>
          Ok(writeSide.aggregateById(key.toString).map(_.asJson))
        case POST -> (Root / writeSideConfig.urlPath.value / "stop") =>
          writeSide.stop.compile.drain *> Ok()
        case POST -> (Root / writeSideConfig.urlPath.value / "local" / "stop") =>
          writeSideProcessor.stop *> Ok()
      }

}
