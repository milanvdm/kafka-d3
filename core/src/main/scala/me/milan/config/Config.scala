package me.milan.config

import cats.effect.Sync
import fs2.Stream
import pureconfig.generic.auto._

object Config {

  def stream[F[_]](
    implicit
    S: Sync[F]
  ): Stream[F, ApplicationConfig] =
    Stream.eval(
      S.delay(
        pureconfig.loadConfigOrThrow[ApplicationConfig]
      )
    )

}
