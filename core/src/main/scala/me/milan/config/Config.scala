package me.milan.config

import cats.effect.Sync
import fs2.Stream
import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Config {

  def stream[F[_]: Sync]: Stream[F, ApplicationConfig] =
    Stream.eval(
      Sync[F].delay(
        ConfigSource.default.loadOrThrow[ApplicationConfig]
      )
    )

}
