package me.milan.store.http

import cats.effect.Sync
import cats.syntax.functor._
import io.circe.Encoder
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import me.milan.config.AggregateStoreConfig
import me.milan.store.AggregateStore

object AggregateStoreRoutes {

  def http4s[F[_]: Sync, A: Encoder](
    aggregateStoreConfig: AggregateStoreConfig,
    aggregateStore: AggregateStore[F, A]
  ): HttpRoutes[F] =
    new Http4sAggregateStoreRoutes[F, A](
      aggregateStoreConfig,
      aggregateStore
    ).routes

}

private[http] class Http4sAggregateStoreRoutes[F[_]: Sync, A: Encoder](
  aggregateStoreConfig: AggregateStoreConfig,
  aggregateStore: AggregateStore[F, A]
) extends Http4sDsl[F] {

  def routes: HttpRoutes[F] =
    HttpRoutes
      .of[F] {
        case GET -> (Root / aggregateStoreConfig.urlPath.value / UUIDVar(key)) =>
          Ok(aggregateStore.byId(key.toString).map(_.asJson))
      }

}
