package me.milan.config

import cats.effect.Sync
import fs2.Stream
import pureconfig.generic.auto._

object Config {

  def stream[F[_]: Sync]: Stream[F, ApplicationConfig] =
    Stream.eval(
      Sync[F].delay(
        pureconfig.loadConfigOrThrow[ApplicationConfig]
      )
    )

}
